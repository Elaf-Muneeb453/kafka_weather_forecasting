# Kafka Weather Forecasting

A small weather-data pipeline that generates site metadata, fetches hourly weather data from [Open-Meteo](https://open-meteo.com/), publishes it to Apache Kafka, and stores it in PostgreSQL.

## Architecture

```text
Open-Meteo API -> producer.py -> Kafka (weather-results) -> consumer.py -> PostgreSQL
```

- `producer.py` creates random sites, requests hourly weather data, and publishes one Kafka message per site.
- `consumer.py` reads Kafka messages and upserts the weather rows into PostgreSQL.
- `common.py` contains shared connection settings and database/table initialization.
- `docker-compose.yml` starts PostgreSQL and Kafka.

## Requirements

- Python 3.10 or newer
- Docker Desktop with Docker Compose
- Internet access for the Open-Meteo API

## Setup

1. Start PostgreSQL and Kafka:

   ```powershell
   docker compose up -d
   ```

2. Create and activate a Python virtual environment:

   ```powershell
   py -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

   If PowerShell blocks activation, run PowerShell with an execution policy that permits local scripts, or activate the environment from another shell.

3. Install the Python dependencies:

   ```powershell
   python -m pip install -r requirements.txt
   ```

## Run the pipeline

Open two terminals in the project directory and activate the virtual environment in both.

Start the consumer first so Kafka messages can be persisted:

```powershell
python consumer.py
```

In the second terminal, start the producer:

```powershell
python producer.py
```

The producer stops after every configured site has all expected hourly rows in PostgreSQL. Both processes retry Kafka and PostgreSQL connections automatically. Press `Ctrl+C` to stop either process.

## Configuration

The defaults are defined in `common.py` and `producer.py`. Override producer settings with environment variables.

| Variable | Default | Description |
| --- | --- | --- |
| `START_DATE` | Today | First date requested from Open-Meteo, in `YYYY-MM-DD` format |
| `END_DATE` | `START_DATE` | Last date requested, inclusive |
| `SITE_COUNT` | `10000` | Number of random sites to create |
| `BATCH_SIZE` | `100` | Number of sites requested per API call |
| `BATCH_DELAY` | `1` | Seconds between API batches |
| `ROUND_DELAY` | `10` | Seconds between retry rounds |

For a small test run in PowerShell:

```powershell
$env:SITE_COUNT = "10"
$env:START_DATE = "2026-01-01"
$env:END_DATE = "2026-01-01"
python producer.py
```

Environment variables apply only to the current terminal session.

## Data storage

Docker exposes PostgreSQL at `localhost:5000` and Kafka at `localhost:9092`.

The application creates these PostgreSQL databases and tables automatically:

- `meta.metadata`: generated site names and coordinates.
- `site_weather.site_weather`: hourly temperature, relative humidity, and direct solar radiation.

The PostgreSQL credentials used by the application and Docker Compose are:

```text
User: postgres
Password: 123789
``` 

The Kafka topic is `weather-results`.

## Useful commands

Check service status:

```powershell
docker compose ps
```

View service logs:

```powershell
docker compose logs -f postgres kafka
```

Stop the services:

```powershell
docker compose down
```

Stop the services and remove persisted database/Kafka data:

```powershell
docker compose down -v
```

The final command permanently deletes the named Docker volumes.
