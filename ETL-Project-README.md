ETL Data Migration Pipeline — CSV + SQLite to PostgreSQL

This project implements a functional, configuration-driven ETL (Extract–Transform–Load) pipeline that migrates data from legacy sources (CSV files + SQLite database) into a PostgreSQL data warehouse.
The target warehouse is modeled using a star schema (dimension + fact tables).

Key Features

1. Extraction

- Extracts CSV files (orders, customers)
- Extracts product data from SQLite database
- Configurable source paths
- Basic schema validation

2. Transformation

- Data cleansing (nulls, formatting)
- Duplicate removal
- Business logic transformations
 (e.g., calculating totals, margins, standardizing fields)

Data validation (required fields, value checks)

3. Loading

- Loads transformed data into PostgreSQL
- Supports full refresh load
- Star-schema warehouse:
- dim_customers
- dim_products
- dim_dates
- fact_orders
- Uses SQL scripts to create tables + date dimension

4. Pipeline Orchestration

- Single orchestrator coordinating Extract → Transform → Load
- Logs each step
- Error handling with context
- Metrics tracking (records extracted, transformed, loaded)

5. Docker-Based Infrastructure

- PostgreSQL + pgAdmin + Metabase running locally via docker-compose
- On startup, database automatically loads warehouse schema

6. Sample Data Generator

- Generates thousands of synthetic customer/product/order records
- Injects data quality issues (5%) to simulate real-world ETL problems

Project Structure

src/
  extract/
     csv_extractor.py
     db_extractor.py
  transform/
     cleaner.py
     validator.py
  load/
     db_loader.py
  pipeline/
     orchestrator.py
  utils/
     error_handler.py
     metrics.py

sql/
  create_tables.sql

data/
  source/
     customers.csv
     orders.csv
     legacy.db

Running the Pipeline
1. Start Postgres + pgAdmin
docker-compose up -d

2. Generate sample data
python generate_sample_data.py

3. Run full ETL pipeline
python -m src.pipeline.orchestrator --mode full

What This Project Demonstrates

ETL design fundamentals
Data pipeline orchestration
Real-world schema design (star schema)
Python-based data engineering
Dockerized warehouse environment
Error handling + configurability