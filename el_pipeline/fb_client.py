import requests
import pandas as pd
import time
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import Config

class FacebookClient:
    def __init__(self):
        self.base_url = f"https://graph.facebook.com/{Config.API_VERSION}"

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def fetch_chunk(self, since_ts, until_ts):
        url = f"{self.base_url}/{Config.FB_PAGE_ID}/insights"
        params = {
            'metric': ','.join(Config.METRICS),
            'period': 'day',
            'since': since_ts,
            'until': until_ts,
            'access_token': Config.FB_ACCESS_TOKEN
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_page_start_date(self):
        url = f"{self.base_url}/{Config.FB_PAGE_ID}"
        params = {'fields': 'created_time', 'access_token': Config.FB_ACCESS_TOKEN} 
        try:
            resp = requests.get(url, params=params).json()
            created_str = resp.get('created_time')
            if created_str:
                return datetime.strptime(created_str[:10], '%Y-%m-%d').date()
        except:
            pass
        return None

    def process_response(self, data):
        rows = []
        if 'data' not in data:
            return pd.DataFrame()
        now_ts = datetime.now()
        for item in data['data']:
            metric_name = item['name']
            for val in item['values']:
                rows.append({
                    'date': val['end_time'][:10],
                    'metric_name': metric_name,
                    'value': val['value'],
                    'ingestion_time': now_ts
                })
        df = pd.DataFrame(rows)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df['ingestion_time'] = pd.to_datetime(df['ingestion_time'])
        return df


    def _get_single_post_data(self, post):
        """
        Helper function: Fetches insights for ONE post.
        This will be run in parallel threads.
        """
        post_id = post.get('id')
        impressions = 0
        engaged_users = 0
        
        try:
            insights_url = f"{self.base_url}/{post_id}/insights"
            i_params = {
                'metric': 'post_impressions,post_engaged_users',
                'access_token': Config.FB_ACCESS_TOKEN
            }
            i_resp = requests.get(insights_url, params=i_params)
            
            if i_resp.status_code == 200:
                i_data = i_resp.json().get('data', [])
                for metric in i_data:
                    val = metric['values'][0]['value']
                    if metric['name'] == 'post_impressions': impressions = val
                    elif metric['name'] == 'post_engaged_users': engaged_users = val
        except:
            pass

        likes = 0
        if 'likes' in post and 'summary' in post['likes']:
            likes = post['likes']['summary'].get('total_count', 0)

        return {
            'post_id': post_id,
            'created_at': post.get('created_time'),
            'message': post.get('message', '')[:200],
            'post_url': post.get('permalink_url'),
            'impressions': int(impressions),
            'engaged_users': int(engaged_users),
            'likes': int(likes),
            'ingestion_time': datetime.now() 
        }

    def fetch_posts_data_optimized(self):
        """
        Fetches ALL posts using Pagination + Multithreading.
        """
        all_rows = []
        

        url = f"{self.base_url}/{Config.FB_PAGE_ID}/posts"
        fields = "id,created_time,message,permalink_url,likes.summary(true).limit(0)"
        params = {
            'fields': fields,
            'limit': 50, 
            'access_token': Config.FB_ACCESS_TOKEN
        }
        
        print("   ...Starting Parallel Full History Extract...")
        
        all_posts_metadata = []
        
        while url:
            try:
                if len(all_posts_metadata) == 0:
                     response = requests.get(url, params=params)
                else:
                     response = requests.get(url)
                
                response.raise_for_status()
                data = response.json()
                
                batch = data.get('data', [])
                if not batch: break
                
                all_posts_metadata.extend(batch)
                print(f"      -> Collected metadata for {len(all_posts_metadata)} posts...")

                if 'paging' in data and 'next' in data['paging']:
                    url = data['paging']['next']
                else:
                    url = None
            except Exception as e:
                print(f"Error fetching post list: {e}")
                break

        print(f"   ...Starting Parallel Insight Fetch for {len(all_posts_metadata)} posts...")

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_post = {executor.submit(self._get_single_post_data, post): post for post in all_posts_metadata}
            
            for i, future in enumerate(as_completed(future_to_post)):
                try:
                    data = future.result()
                    all_rows.append(data)
                except Exception as exc:
                    print(f"      -> Generated an exception: {exc}")
                
                if (i + 1) % 50 == 0:
                    print(f"      -> Processed {i + 1}/{len(all_posts_metadata)} posts")

        df = pd.DataFrame(all_rows)
        if not df.empty:
            df['created_at'] = pd.to_datetime(df['created_at'])
            df['ingestion_time'] = pd.to_datetime(df['ingestion_time'])
            
        return df