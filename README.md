# Amazon Sales Data Engineering Pipeline

An end-to-end Data Engineering project that ingests raw Amazon sales data, processes it using PySpark following a **Bronze–Silver–Gold** architecture, orchestrates workflows with **Apache Airflow**, and stores analytics-ready outputs in **PostgreSQL**, fully containerized using **Docker**.

---

## Tech Stack

- **Language:** Python  
- **Processing Engine:** Apache Spark (PySpark)  
- **Orchestration:** Apache Airflow  
- **Database:** PostgreSQL  
- **Containerization:** Docker, Docker Compose  
- **Data Source:** Kaggle – Amazon Sales Dataset  

---

## Architecture Overview

This project follows a **layered data architecture** commonly used in production data platforms.

- **Bronze Layer:** Raw data ingestion (CSV)
- **Silver Layer:** Cleaned, validated, and normalized data
- **Gold Layer:** Business-ready aggregated tables for analytics

---

## Project Structure

```text
project-root/
│
├── dags/
│   └── amazon_etl_dag.py
│       Airflow DAG that orchestrates the end-to-end ETL pipeline
│
├── spark/
│   └── jobs/
│       ├── extract.py
│       │   Reads raw CSV data into Spark
│       │
│       ├── transform.py
│       │   Performs cleaning, validation, type casting, and enrichment
│       │
│       └── gold_aggregation.py
│           Creates analytics-ready gold tables
│
├── sql/
│   ├── create_product_summary.sql
│   ├── create_category_summary.sql
│   ├── create_top_products.sql
│   └── create_discount_effectiveness.sql
│   SQL scripts to create gold-layer tables in PostgreSQL
│
├── data/
│   └── raw/
│       Contains raw input data  
│   └── processed/
│       Contains processed data
│   └── raw/
│       Contains analytics read aggregated data
|
├── docker/
│   ├── Dockerfile
│   │   Custom image with Airflow, Spark, and project dependencies
│   │
│   └── docker-compose.yml
│       Defines services for Airflow, PostgreSQL, and networking
│
├── logs/
│   Airflow execution logs
│
├── plugins/
│   Airflow plugins (if any)
│
├── README.md
│   Project documentation
│
└── .gitignore
```
---

## Data Layers Explained

### Bronze Layer (Extract)
- Ingests raw CSV data exactly as received
- No transformations or business logic applied
- Preserves original data for traceability and reprocessing
- Acts as the single source of truth

### Silver Layer (Transform)
- Performs data cleaning and validation
- Handles:
  - Null value checks
  - Data type casting
  - Derived columns such as rating_bucket, discount_bucket, price_difference, and rating_strength
- Produces clean, reliable, and analysis-ready datasets

### Gold Layer (Gold Aggregation)
- Applies business logic and aggregations
- Creates analytics-ready tables
- Optimized for reporting and downstream consumption
- Final outputs are written to PostgreSQL as well as parquet file

---

## Gold Tables

### product_summary
- Category-level aggregation
- Metrics include:
  - follow the sql/ folder

### category_summary
- Overall category performance insights
- Helps identify high- and low-performing categories

### top_products
- Identifies top-ranked products of each category
- Ranking based on rating strength and engagement
- Useful for merchandising and recommendation analysis

### discount_effectiveness
- Analyzes the relationship between discount levels and:
  - Average rating
  - Average rating count
- Supports pricing and promotion strategy decisions

---

## Orchestration Flow

1. Airflow DAG is triggered manually or via schedule
2. Bronze ingestion job reads raw CSV data from kaggle
3. Silver transformation job cleans, validates, and enriches the data
4. Gold aggregation job generates business-level tables
5. Gold tables are written to PostgreSQL

---

## How to Run the Project

### Prerequisites
- Docker
- Docker Compose

### Steps

```bash
docker compose up --build
```

- Airflow UI: http://localhost:8080  
- Default credentials:
  - Username: airflow
  - Password: airflow

Trigger the DAG:
- amazon_sales_etl

---

## Dataset Notes
Find the dataset from Kaggle:
- https://www.kaggle.com/datasets/karkavelrajaj/amazon-sales-dataset
- No need to download manually and place into project. ETL do it automatic, just run the DAG

---

## Business Problems Solved

- Identify top-performing product categories using aggregated rating, review, and pricing metrics  
- Analyze the impact of discounts on customer ratings and review behavior  
- Discover products with strong customer engagement based on rating volume and rating strength  
- Provide analytics-ready gold tables for BI tools and reporting use cases  

---

## Why This Project Matters

This project demonstrates:

- Production-style ETL pipeline design aligned with real-world data platforms  
- Clear separation of concerns using a Bronze–Silver–Gold layered architecture  
- Workflow orchestration and dependency management using Apache Airflow  
- Scalable and distributed data processing with Apache Spark  
- Fully containerized deployment using Docker and Docker Compose  

---

## Future Improvements

- Add automated data quality checks and alerting mechanisms  
- Implement incremental loading and CDC-based pipelines  
- Integrate cloud storage solutions such as Amazon S3
- Connect downstream BI tools like Power BI or Apache Superset for visualization  

