# Social Analytics dbt Project

This dbt project transforms raw Facebook and GA4 data into analytics-ready tables.

## Project Structure

```
dbt_social_analytics/
├── models/
│   ├── staging/          # Clean raw data
│   │   ├── stg_page_insights.sql
│   │   └── stg_posts.sql
│   └── marts/            # Business-ready tables
│       ├── fct_daily_page_metrics.sql
│       ├── fct_post_analytics.sql
│       └── dim_date.sql
```

## Setup

1. Install dbt-bigquery:
   ```bash
   pip install dbt-bigquery
   ```

2. Copy `profiles.yml` to `~/.dbt/profiles.yml` or set `DBT_PROFILES_DIR`:
   ```bash
   export DBT_PROFILES_DIR=./
   ```

3. Set your GCP credentials:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/key.json
   ```

4. Test connection:
   ```bash
   dbt debug
   ```

## Running Models

```bash
# Run all models
dbt run

# Run specific model
dbt run --select fct_post_analytics

# Run with tests
dbt build

# Generate docs
dbt docs generate
dbt docs serve
```

## Models

| Model | Type | Description |
|-------|------|-------------|
| `stg_page_insights` | View | Cleaned page metrics with deduplication |
| `stg_posts` | View | Cleaned posts with date extraction |
| `fct_daily_page_metrics` | Table | Pivoted daily metrics |
| `fct_post_analytics` | Table | Enriched posts with engagement rates |
| `dim_date` | Table | Date dimension |
