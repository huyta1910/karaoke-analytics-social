import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

load_dotenv()


GOOGLE_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
BQ_DATASET_ID = os.getenv('BQ_DATASET_ID') 
BQ_TABLE_ID = "ga4_historical_summary"     
GA4_PROPERTY_ID = "YOUR_NUMERIC_PROPERTY_ID_HERE" 
START_DATE = "2023-01-01"  
END_DATE = "2024-05-20"    

ga4_client = BetaAnalyticsDataClient()
bq_client = bigquery.Client(project=BQ_PROJECT_ID)

# --- 2. FETCH FUNCTION ---
def fetch_ga4_data(start_date, end_date):
    print(f"   ...Fetching GA4 data from {start_date} to {end_date}...")
    
    request = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="date")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="activeUsers"),
            Metric(name="screenPageViews"),
            Metric(name="eventCount")
        ],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )

    try:
        response = ga4_client.run_report(request)
    except Exception as e:
        print(f"GA4 API Error: {e}")
        return pd.DataFrame()

    data = []
    for row in response.rows:
        data.append({
            "date": row.dimension_values[0].value, 
            "sessions": int(row.metric_values[0].value),
            "active_users": int(row.metric_values[1].value),
            "page_views": int(row.metric_values[2].value),
            "event_count": int(row.metric_values[3].value),
            "ingestion_time": datetime.now()
        })

    df = pd.DataFrame(data)
    
    if not df.empty:
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df['ingestion_time'] = pd.to_datetime(df['ingestion_time'])
    
    return df

def upload_to_bigquery(df):
    if df.empty:
        return

    table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("sessions", "INTEGER"),
            bigquery.SchemaField("active_users", "INTEGER"),
            bigquery.SchemaField("page_views", "INTEGER"),
            bigquery.SchemaField("event_count", "INTEGER"),
            bigquery.SchemaField("ingestion_time", "TIMESTAMP"),
        ],
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
    )

    try:
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Uploaded {len(df)} rows to {BQ_TABLE_ID}")
    except Exception as e:
        print(f"BigQuery Upload Failed: {e}")

def main():
    print(f"Starting GA4 History Backfill ({START_DATE} to {END_DATE})")
    
    start = datetime.strptime(START_DATE, "%Y-%m-%d").date()
    end = datetime.strptime(END_DATE, "%Y-%m-%d").date()
    
    current = start
    chunk_size = 30
    
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_size), end)
        
    
        s_str = current.strftime("%Y-%m-%d")
        e_str = chunk_end.strftime("%Y-%m-%d")
        
        df = fetch_ga4_data(s_str, e_str)
        
        if not df.empty:
            upload_to_bigquery(df)
        else:
            print("(No data found in this chunk)")
            
        current = chunk_end + timedelta(days=1)

    print("Backfill Complete.")

if __name__ == "__main__":
    main()