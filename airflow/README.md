# Airflow Setup for Social Analytics

This directory contains the Airflow DAG for automating the social analytics pipeline.

## Pipeline Flow

```
Ingestion (Python) → dbt run → dbt test → dbt docs generate
```

Runs daily at 6 AM.

## Local Setup with Docker

1. Create credentials directory and add your GCP key:
   ```bash
   mkdir -p credentials
   cp /path/to/your-gcp-key.json credentials/gcp-key.json
   ```

2. Start Airflow:
   ```bash
   docker-compose up -d
   ```

3. Access Airflow UI at `http://localhost:8080`
   - Username: `admin`
   - Password: `admin`

4. Enable the `social_analytics_pipeline` DAG

## Cloud Composer (GCP Managed Airflow)

To deploy to Cloud Composer:

1. Upload the DAG file to your Composer bucket:
   ```bash
   gsutil cp dags/social_analytics_dag.py gs://your-composer-bucket/dags/
   ```

2. Set environment variables in Composer:
   - `FB_PAGE_ID`
   - `FB_ACCESS_TOKEN`
   - `BQ_PROJECT_ID`
   - `BQ_DATASET_ID`
   - `BQ_TABLE_ID`

3. The DAG will automatically appear in the Airflow UI
