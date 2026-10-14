# E-commerce Data Engineering Project

## Project Overview

An end-to-end e-commerce data engineering project built using Databricks, PySpark, and Delta Lake. This project transforms raw CSV files into clean, analytics-ready datasets using the Medallion Architecture.

## Architecture

```text
Raw CSV Files
     |
     v
Bronze Layer
Raw data ingestion
     |
     v
Silver Layer
Data cleaning and standardization
     |
     v
Gold Layer
Sales details and aggregated summaries
     |
     v
Business Analytics
```

## Technologies Used

- **Databricks** — Data processing and workflow orchestration
- **PySpark** — Data transformation
- **Delta Lake** — Table storage and reliable data writes
- **Python** — Transformation logic
- **GitHub** — Version control and project documentation

## Project Structure

```text
ecommerce-data-engineering/
├── ecommerce_bronze_ingestion.py
├── ecommerce_silver_transformation.py
├── ecommerce_gold_transformation.py
└── README.md
```

## Data Pipeline

### 1. Bronze — Data Ingestion
- Loads raw e-commerce CSV files into Bronze tables.
- Preserves source data for downstream processing.

### 2. Silver — Data Transformation
- Cleans and standardizes order, customer, product, brand, category, and date data.
- Handles data type conversions and inconsistent values.
- Prepares datasets for analytical use.

### 3. Gold — Business-Level Data
- Joins sales with product, brand, category, customer, and date information.
- Calculates gross sales and discount amounts.
- Creates aggregated sales summaries by date, currency, and sales channel.

## Results

The Databricks pipeline completed successfully with all three tasks succeeding.

| Gold table | Rows |
|---|---:|
| `sales_detail` | 183,378 |
| `sales_summary` | 1,288 |

The sales summary groups data by date, currency, and channel. Different currencies are kept separate to avoid misleading revenue comparisons.

## How to Run

1. Import the three Python notebooks into a Databricks workspace.
2. Upload the required source CSV files to an accessible storage location.
3. Configure the required Unity Catalog catalog, schemas, and table permissions.
4. Run Bronze ingestion first, followed by Silver and Gold transformations.
5. Configure a Databricks Job with dependencies in this order:

   `bronze_ingestion → silver_transformation → gold_transformation`

## Future Improvements

- Build a sales analytics dashboard.
- Add automated data quality checks.
- Add incremental processing for newly arriving files.
- Integrate cloud object storage.
- Add scheduled pipeline execution and monitoring.

## Author

**Mohammed Mahaboob**

GitHub: [mahboob-codes](https://github.com/mahboob-codes)
