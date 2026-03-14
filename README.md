American E-commerce Data Engineering Pipeline
---------------------------------------------

Overview
--------

This project implements a data engineering pipeline to ingest, transform, and model an e-commerce dataset for analytics.

The pipeline processes raw CSV files and transforms them into a structured analytical model using a Star Schema.

Dataset:
--------

American E-commerce Public Dataset from Kaggle.

Tables include:
---------------

1. olist_customers_dataset - Customers data
2. olist_geolocation_dataset - Geolocation data
3. olist_order_items_dataset - Ordered items data
4. olist_order_payments_dataset - Order payments information
5. olist_order_reviews_dataset - Order reviews information
6. olist_orders_dataset - Orders data
7. olist_products_dataset - Products data
8. olist_sellers_dataset - Sellers data
9. product_category_name_translation - Product Category Name information


Architecture:
-------------

Click/copy-paste this link for the complete Architecture and the Star Schema Data Model: https://www.figma.com/board/OMFFTd2lUbkkias25BUK72/End-to-End-data-pipeline?node-id=0-1&t=oK9eqSEO4EItjG1N-1

The pipeline consists of the following stages:

1. Data ingestion using PySpark
2. Data cleaning and transformation
3. Star schema data modeling
4. Aggregated analytical tables
5. Storage in Parquet format
6. SQL analysis queries
7. Pipeline orchestration via Cron-style script

Project Structure:
------------------

data-engineering-project
│
├── data
│   ├── raw
│   └── processed
│
├── pipelines
│   ├── ingestion.py
│   ├── transformation.py
│   └── run_pipeline.py
│
├── sql
│   └── analysis_queries.sql
│
├── README.md

Data Model:
-----------

The warehouse follows a Star Schema design with:

Fact Table:

* sales_fact

Dimension Tables:

* customer_dim
* product_dim
* date_dim
* sellers_dim

Analytical Queries:
-------------------

1. To find the top 10 selling products
2. To find the monthly revenue
3. To find the customer purchase frequency

Queries are available in:
-------------------------
sql/analysis_queries.sql

How to Run the Pipeline?
------------------------

1. Install dependencies: pip install -r requirements.txt

2. Run the pipeline: python pipelines/run_pipeline.py

This will execute:

* data ingestion
* transformation
* star schema generation
* analytics table creation

Technologies Used:
------------------
The following technologies were used to build the pipeline:

1. Programming Language: Python
2. Data Processing: Apache Spark (PySpark)
3. Data Storage: Parquet
4. Data Modeling: Star Schema
5. Query Layer: SQL for analytical queries
6. Pipeline Orchestration: Airflow
7. Development Tools: 
	a. Git for Version Control
	b. GitHub for Repository
	c. IntelliJ IDEA for development

