# spark-medallion-pipeline

Batch ETL pipeline built with PySpark and Delta Lake, following the Medallion architecture (Bronze / Silver / Gold) — ingesting NYC Taxi data from S3.

## Architecture
```
Bronze (raw Delta table on S3)
    ↓
Silver (cleaned & validated Delta table)
    ↓
Gold (aggregated tables ready for analytics)
```

## Stack

- **PySpark** — distributed data processing
- **Delta Lake** — ACID transactions & versioning on S3
- **MySQL** — source OLTP database
- **LocalStack** — local AWS S3 emulation
- **Docker Compose** — local orchestration
- **uv** — Python package manager

## Project Structure
```
spark-medallion-pipeline/
├── docker/
│   └── docker-compose.yml
├── src/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── data/
│   └── init/          # SQL seed scripts
├── tests/
├── .env
├── pyproject.toml
└── README.md
```

## Getting Started
```bash
# Clone the repo
git clone https://github.com/<your-username>/spark-medallion-pipeline

# Start the infrastructure
docker compose -f docker/docker-compose.yml up -d

# Run the pipeline
uv run src/main.py
```

## Dataset

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — Yellow Taxi trips loaded into MySQL as the source system.