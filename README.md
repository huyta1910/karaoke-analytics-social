# Karaoke Analytics Social Pipeline

A robust data engineering pipeline designed to collect, process, and analyze social media engagement and web traffic for karaoke business performance monitoring.

## Overview

This project implements an **ELT (Extract, Load, Transform)** architecture. It automates the collection of data from Facebook (Page Insights & Post Performance) and Google Analytics 4 (GA4), centralizes it in Google BigQuery, and transforms it into business-ready analytical datasets using dbt.

## Architecture

The pipeline consists of three main stages:

1.  **Orchestration**: Managed by **Apache Airflow** running in Docker. Airflow schedules daily jobs and handles dependencies between data ingestion and transformation.
2.  **Ingestion (Extract & Load)**: Custom Python scripts (`el_pipeline`) interface with:
    *   **Facebook Graph API**: Fetches daily page-level metrics and individual post statistics.
    *   **GA4 Data API**: Retrieves sessions, active users, and event counts.
3.  **Transformation (Transform)**: **dbt (data build tool)** structured in a modern data stack pattern:
    *   **Source/Staging**: Direct mappings from raw BigQuery tables with basic cleaning.
    *   **Marts**: Business-logic-heavy models (Facts & Dimensions) optimized for BI tools.

## Tech Stack

*   **Data Orchestration**: Apache Airflow
*   **Data Warehouse**: Google BigQuery
*   **Transformation**: dbt (Core)
*   **Languages**: Python 3.10+, SQL (BigQuery Dialect)
*   **Infrastructure**: Docker & Docker Compose
*   **APIs**: Facebook Graph API, GA4 Beta Analytics Data API

## Schema Design

The project follows a **Star Schema** design in the `marts` layer to facilitate easy reporting.

### 1. Ingestion Layer (Raw Data)
*   **`page_insights_daily`**: Stores time-series data of page-level engagement (e.g., views, likes, reach).
*   **`post_performance`**: Snapshot of individual post metrics (impressions, engaged users, clicks).
*   **`ga4_historical_summary`**: Cumulative web traffic metrics including sessions and active users.

### 2. Transformation Layer (dbt Staging)
*   **`stg_page_insights`**: Cleans raw JSON-like responses, handles deduplication, and casts timestamps.
*   **`stg_posts`**: Extracts IDs and URLs, truncates long message strings, and standardizes metric names.

### 3. Analytics Layer (dbt Marts)
*   **`dim_date`**: A rich date dimension supporting day-of-week, month, and year groupings for seasonal trend analysis.
*   **`fct_daily_page_metrics`**: A daily grain fact table that pivots metric names into columns (e.g., `impressions`, `reach`, `engagements`) for easier calculation in BI tools.
*   **`fct_post_analytics`**: A granular fact table calculating performance ratios like **Engagement Rate** (`engaged_users / impressions`) per post.

## Data Flow

1.  **Airflow DAG** triggers daily at 06:00 AM.
2.  **`run_ingestion`**:
    *   Checks BigQuery for the last ingested date.
    *   Fetches incremental data from Facebook/GA4 APIs.
    *   Uploads raw data to BigQuery.
3.  **`run_dbt_models`**:
    *   Executes dbt transformations.
    *   Materializes staging tables and final facts/dimensions.
4.  **`run_dbt_tests`**:
    *   Validates data integrity (null checks, unique keys, relationship tests).
5.  **`generate_dbt_docs`**:
    *   Updates the dbt documentation site.

## Setup & Local Development

### Prerequisites
*   Docker & Docker Compose
*   Google Cloud Service Account (JSON key) with BigQuery Admin permissions.
*   Facebook Page Access Token.

### Environment Configuration
Create a `.env` file in the root with the following variables:
```env
FB_PAGE_ID=your_id
FB_ACCESS_TOKEN=your_token
BQ_PROJECT_ID=your_project
BQ_DATASET_ID=your_dataset
BQ_TABLE_ID=your_table
```

### Running the Pipeline
```bash
docker-compose up -d

docker-compose run airflow-init
```

Access the Airflow UI at `http://localhost:8080` (default credentials: `admin`/`admin`).
