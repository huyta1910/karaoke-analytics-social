import time
from datetime import datetime, timedelta
from fb_client import FacebookClient
from bigquery_client import BigQueryClient
from config import Config

def main():
    print("--- Starting Analytics Pipeline ---")
    
    fb = FacebookClient()
    bq = BigQueryClient()

    print("\n--- 1. Processing PAGE Insights ---")
    last_bq_date = bq.get_last_ingested_date()
    
    if last_bq_date:
        print(f"Found existing data up to: {last_bq_date}")
        start_date = last_bq_date + timedelta(days=1)
    else:
        print("No existing data found. Fetching Page creation date...")
        page_created = fb.get_page_start_date()
        start_date = page_created if page_created else (datetime.now() - timedelta(days=365)).date()

    end_date = datetime.now().date()
    
    if start_date < end_date:
        print(f"Backfilling from {start_date} to {end_date}...")
        current_date = start_date
        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=Config.DATE_CHUNK_SIZE_DAYS), end_date)
            since_ts = int(time.mktime(current_date.timetuple()))
            until_ts = int(time.mktime(chunk_end.timetuple()))

            try:
                raw_data = fb.fetch_chunk(since_ts, until_ts)
                df = fb.process_response(raw_data)
                if not df.empty:
                    bq.upload_data(df)
            except Exception as e:
                print(f"Error processing chunk {current_date}: {e}")
                break 
            current_date = chunk_end
    else:
        print("Page Insights are up to date.")



    print("\n--- 2. Processing POST Analytics ---")
    print("Fetching last 50 posts...")
    
    print("\n--- 2. Processing POST Analytics (Optimized) ---")
    
    try:
        # Use the OPTIMIZED function
        df_posts = fb.fetch_posts_data_optimized()
        
        if not df_posts.empty:
            bq.upload_posts_data(df_posts)
        else:
            print("No posts found.")

    except Exception as e:
        print(f" Post processing failed: {e}")

    print("\n--- Pipeline Finished ---")

if __name__ == "__main__":
    main()