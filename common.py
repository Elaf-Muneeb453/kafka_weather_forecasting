import os
import time
import logging

from datetime import date

import psycopg
from psycopg import sql

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger("weather-kafka")

# PostgreSQL configuration - preserved from the original assignment project
HOST = "localhost"
PORT = "5000"
USER = "postgres"
PASSWORD = "123789"

# Apache Kafka configuration
BROKER = "localhost:9092"
TOPIC = "weather-results"

START_DATE = os.getenv("START_DATE", date.today().isoformat())

END_DATE = os.getenv("END_DATE", START_DATE)


def connect(db):
    return psycopg.connect(
        dbname=db,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        connect_timeout=10,
    )


def retry(fn, name):
    while True:
        try:
            return fn()

        except (psycopg.OperationalError, OSError) as e:
            LOG.warning("%s unavailable: %s. Retrying in 5 seconds.", name, e)
            time.sleep(5)


def setup():
    def create():
        # Keep the original assignment database flow.
        # csv_database is created by docker-compose on first PostgreSQL startup.
        with connect("csv_database") as conn:
            conn.autocommit = True

            with conn.cursor() as cur:
                for db in ("meta", "site_weather"):
                    cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (db,))

                    if not cur.fetchone():
                        cur.execute(
                            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db))
                        )
                        print(f"Database {db} created.")
                    else:
                        print(f"Database {db} already exists.")

        # Create metadata table
        with connect("meta") as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    site_name VARCHAR(100) PRIMARY KEY,
                    latitude DOUBLE PRECISION NOT NULL,
                    longitude DOUBLE PRECISION NOT NULL,
                    UNIQUE(latitude, longitude)
                )
            """)

        # Create weather table
        with connect("site_weather") as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS site_weather (
                    site_name VARCHAR(100) NOT NULL,
                    time_interval TIMESTAMP NOT NULL,
                    temperature DOUBLE PRECISION,
                    humidity DOUBLE PRECISION,
                    solar_radiance DOUBLE PRECISION,
                    PRIMARY KEY(site_name, time_interval)
                )
            """)

        print("Database setup completed successfully.")

    retry(create, "PostgreSQL")
