import json
import os
import random
import string
import time
from datetime import date, timedelta

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from common import BROKER, END_DATE, START_DATE, TOPIC, connect, setup

SITE_COUNT = int(os.getenv("SITE_COUNT", "10000"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
BATCH_DELAY = float(os.getenv("BATCH_DELAY", "1"))
ROUND_DELAY = int(os.getenv("ROUND_DELAY", "10"))
API = "https://api.open-meteo.com/v1/forecast"

EXPECTED_ROWS = (
    date.fromisoformat(END_DATE) - date.fromisoformat(START_DATE)
).days * 24 + 24
END_EXCLUSIVE = (date.fromisoformat(END_DATE) + timedelta(days=1)).isoformat()


def make_sites():
    """Create random metadata until SITE_COUNT distinct sites exist."""
    while True:
        try:
            with connect("meta") as db:
                count = db.execute("SELECT COUNT(*) FROM metadata").fetchone()[0]
                while count < SITE_COUNT:
                    name = "".join(random.choices(string.ascii_uppercase, k=3)) + "".join(
                        random.choices(string.digits, k=3)
                    )
                    # Avoid exact poles, which are less useful for weather sampling.
                    lat = round(random.uniform(-89.9, 89.9), 2)
                    lon = round(random.uniform(-179.9, 179.9), 2)
                    row = db.execute(
                        """
                        INSERT INTO metadata (site_name, latitude, longitude)
                        VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                        RETURNING site_name
                        """,
                        (name, lat, lon),
                    ).fetchone()
                    if row:
                        count += 1
                print(f"Metadata ready: {count}/{SITE_COUNT} sites")
                return
        except Exception as exc:
            print(f"Metadata database error: {exc}. Retrying in 5 seconds...")
            time.sleep(5)


def all_sites():
    while True:
        try:
            with connect("meta") as db:
                return db.execute(
                    "SELECT site_name, latitude, longitude FROM metadata ORDER BY site_name"
                ).fetchall()
        except Exception as exc:
            print(f"Could not read metadata: {exc}. Retrying in 5 seconds...")
            time.sleep(5)


def complete_site_names():
    while True:
        try:
            with connect("site_weather") as db:
                rows = db.execute(
                    """
                    SELECT site_name
                    FROM site_weather
                    WHERE time_interval >= %s
                      AND time_interval < %s
                    GROUP BY site_name
                    HAVING COUNT(DISTINCT time_interval) = %s
                    """,
                    (START_DATE, END_EXCLUSIVE, EXPECTED_ROWS),
                ).fetchall()
                return {row[0] for row in rows}
        except Exception as exc:
            print(f"Completion check failed: {exc}. Retrying in 5 seconds...")
            time.sleep(5)


def fetch(batch):
    params = {
        "latitude": ",".join(str(site[1]) for site in batch),
        "longitude": ",".join(str(site[2]) for site in batch),
        "hourly": "temperature_2m,relative_humidity_2m,direct_radiation",
        "start_date": START_DATE,
        "end_date": END_DATE,
        "timezone": "GMT",
    }

    for attempt in range(1, 6):
        try:
            response = requests.get(API, params=params, timeout=(10, 60))

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After", "60")
                try:
                    wait = min(max(int(retry_after), 1), 300)
                except ValueError:
                    wait = 60
                print(f"API rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue

            if response.status_code >= 500:
                raise requests.HTTPError(
                    f"Open-Meteo server error {response.status_code}", response=response
                )

            response.raise_for_status()
            result = response.json()
            result = result if isinstance(result, list) else [result]

            if len(result) != len(batch):
                raise ValueError(
                    f"API returned {len(result)} result(s) for {len(batch)} site(s)"
                )
            return result

        except (requests.RequestException, ValueError) as exc:
            wait = min(2 ** (attempt - 1), 30)
            print(f"API attempt {attempt}/5 failed: {exc}. Retrying in {wait}s...")
            time.sleep(wait)

    return None


def valid_rows(site_name, weather):
    if not isinstance(weather, dict) or weather.get("error"):
        return None

    hourly = weather.get("hourly", {})
    keys = ["time", "temperature_2m", "relative_humidity_2m", "direct_radiation"]

    if any(
        not isinstance(hourly.get(key), list) or len(hourly[key]) != EXPECTED_ROWS
        for key in keys
    ):
        return None

    rows = []
    for timestamp, temperature, humidity, radiation in zip(
        hourly["time"],
        hourly["temperature_2m"],
        hourly["relative_humidity_2m"],
        hourly["direct_radiation"],
    ):
        if None in (timestamp, temperature, humidity, radiation):
            return None
        rows.append([site_name, timestamp, temperature, humidity, radiation])

    return rows if len(rows) == EXPECTED_ROWS else None


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BROKER,
                acks="all",
                retries=20,
                retry_backoff_ms=1000,
                request_timeout_ms=60000,
                api_version_auto_timeout_ms=10000,
                max_in_flight_requests_per_connection=1,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
            producer.bootstrap_connected()
            print(f"Connected to Apache Kafka at {BROKER}")
            return producer
        except (NoBrokersAvailable, KafkaError, OSError) as exc:
            print(f"Apache Kafka unavailable: {exc}. Retrying in 5 seconds...")
            time.sleep(5)


def send_site(producer, site_name, rows):
    while True:
        try:
            producer.send(
                TOPIC,
                {"site_name": site_name, "rows": rows},
            ).get(timeout=90)
            print(f"Kafka accepted {site_name}: {len(rows)} hourly records")
            return producer
        except Exception as exc:
            print(f"Kafka send failed for {site_name}: {exc}. Reconnecting...")
            try:
                producer.close(timeout=5)
            except Exception:
                pass
            time.sleep(5)
            producer = create_producer()


def main():
    if date.fromisoformat(END_DATE) < date.fromisoformat(START_DATE):
        raise ValueError("END_DATE must not be before START_DATE")

    setup()
    make_sites()
    sites = all_sites()
    producer = create_producer()
    round_number = 0

    try:
        while True:
            done = complete_site_names()
            pending = [site for site in sites if site[0] not in done]

            if not pending:
                print("\nSUCCESS: every site has all expected rows in PostgreSQL.")
                print(f"Sites: {len(sites)}")
                print(f"Rows per site: {EXPECTED_ROWS}")
                print(f"Expected total rows: {len(sites) * EXPECTED_ROWS}")
                return

            round_number += 1
            print(
                f"\nRetry round {round_number}: "
                f"complete={len(done)}, pending={len(pending)}"
            )

            for start in range(0, len(pending), BATCH_SIZE):
                batch = pending[start : start + BATCH_SIZE]
                result = fetch(batch)

                for index, site in enumerate(batch):
                    site_name = site[0]
                    rows = valid_rows(site_name, result[index]) if result else None

                    # A bad batch result should not lose the site. Try it alone.
                    if rows is None:
                        print(f"Batch data invalid for {site_name}; retrying site alone...")
                        single = fetch([site])
                        rows = valid_rows(site_name, single[0]) if single else None

                    if rows is None:
                        # Never permanently skip it. The next outer round retries it.
                        print(f"Deferred {site_name}; it will be retried automatically.")
                        continue

                    producer = send_site(producer, site_name, rows)

                time.sleep(BATCH_DELAY)
                
            try:
                producer.flush(timeout=60)
            except Exception as exc:
                print(f"Kafka flush warning: {exc}")

            print(f"Round {round_number} sent. Waiting {ROUND_DELAY}s for consumer writes...")
            time.sleep(ROUND_DELAY)

    finally:
        try:
            producer.flush(timeout=30)
        except Exception:
            pass
        try:
            producer.close(timeout=10)
        except Exception:
            pass


if __name__ == "__main__":
    while True:
        try:
            main()
            break
        except KeyboardInterrupt:
            print("Producer stopped by user.")
            break
        except Exception as exc:
            print(f"Unexpected producer error: {exc}. Restarting in 5 seconds...")
            time.sleep(5)


