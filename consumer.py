import json
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

from common import BROKER, TOPIC, connect, setup

def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BROKER,
                group_id="weather-savers",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                request_timeout_ms=60000,
                max_poll_interval_ms=300000,
            )
            print(f"Connected to Apache Kafka at {BROKER}; waiting for weather data...")
            return consumer
        except (NoBrokersAvailable, KafkaError, OSError) as exc:
            print(f"Apache Kafka unavailable: {exc}. Retrying in 5 seconds...")
            time.sleep(5)


def save_message(data):
    if not isinstance(data, dict):
        raise ValueError("Kafka message is not a JSON object")

    site_name = data.get("site_name")
    rows = data.get("rows")

    if not site_name or not isinstance(rows, list) or not rows:
        raise ValueError("Kafka message is missing site_name or rows")

    if any(not isinstance(row, list) or len(row) != 5 or row[0] != site_name for row in rows):
        raise ValueError("Kafka message contains invalid weather rows")

    with connect("site_weather") as db:
        with db.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO site_weather
                    (site_name, time_interval, temperature, humidity, solar_radiance)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (site_name, time_interval)
                DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    humidity = EXCLUDED.humidity,
                    solar_radiance = EXCLUDED.solar_radiance
                """,
                rows,
            )
        # psycopg context commits only after the full write succeeds.


def main():
    setup()

    while True:
        consumer = create_consumer()
        try:
            for message in consumer:
                # Do not commit the Kafka offset until PostgreSQL definitely saved it.
                while True:
                    try:
                        save_message(message.value)
                        consumer.commit()
                        print(
                            f"Saved {message.value['site_name']}: "
                            f"{len(message.value['rows'])} records"
                        )
                        break
                    except KeyboardInterrupt:
                        raise
                    except Exception as exc:
                        print(f"Database/save error: {exc}. Retrying same message in 5s...")
                        time.sleep(5)

        except KeyboardInterrupt:
            print("Consumer stopped by user.")
            return
        except Exception as exc:
            print(f"Kafka consumer error: {exc}. Reconnecting in 5 seconds...")
            time.sleep(5)
        finally:
            try:
                consumer.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
