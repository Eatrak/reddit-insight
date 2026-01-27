
import json
import time
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import random

# Configuration
ES_HOST = "http://elasticsearch:9200"
API_HOST = "http://trend-api:8000"
DAYS_BACK = 10

def get_all_topics():
    import requests
    try:
        r = requests.get(f"{API_HOST}/topics")
        r.raise_for_status()
        return [t["id"] for t in r.json()]
    except Exception as e:
        print(f"Failed to fetch topics: {e}")
        return []

def generate_data():
    es = Elasticsearch(ES_HOST, basic_auth=("admin", "admin"))
    print(f"Connected to ES at {ES_HOST}")
    
    topic_ids = get_all_topics()
    print(f"Found {len(topic_ids)} topics to backfill: {topic_ids}")
    
    actions = []
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    for topic_id in topic_ids:
        # Randomized baseline for each topic so they look different
        base_mentions = random.randint(50, 500)
        base_engagement = base_mentions * random.randint(10, 50)
        
        for i in range(DAYS_BACK):
            start_date = now - timedelta(days=i)
            end_date = start_date + timedelta(days=1)
            
            # Trend shape: Random walk
            daily_mentions = int(base_mentions * (1 + random.uniform(-0.4, 0.6)))
            daily_engagement = int(base_engagement * (daily_mentions / base_mentions) * random.uniform(0.8, 1.2))
            
            doc = {
                "_index": "reddit-topic-metrics",
                "_source": {
                    "@timestamp": datetime.now(timezone.utc).isoformat(),
                    "@version": "1",
                    "topic_id": topic_id,
                    "window_type": "1d",
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat(),
                    "mentions": daily_mentions,
                    "engagement": daily_engagement,
                    "velocity": round(random.uniform(-0.5, 0.5), 2),
                    "acceleration": round(random.uniform(-0.1, 0.1), 2)
                }
            }
            actions.append(doc)
            
        # 1m Aggregate
        month_start = now - timedelta(days=30)
        month_end = now
        total_m = sum(a['_source']['mentions'] for a in actions if a['_source']['topic_id'] == topic_id)
        total_e = sum(a['_source']['engagement'] for a in actions if a['_source']['topic_id'] == topic_id)
        
        actions.append({
            "_index": "reddit-topic-metrics",
            "_source": {
                 "@timestamp": datetime.now(timezone.utc).isoformat(),
                 "@version": "1",
                 "topic_id": topic_id,
                 "window_type": "1m",
                 "start": month_start.isoformat(),
                 "end": month_end.isoformat(),
                 "mentions": total_m * 2, 
                 "engagement": total_e * 2,
                 "velocity": 0,
                 "acceleration": 0
            }
        })

    # Bulk insert
    if actions:
        print(f"Inserting {len(actions)} documents...")
        success, failed = bulk(es, actions)
        print(f"Done! Success: {success}, Failed: {failed}")
    else:
        print("No actions to insert.")

if __name__ == "__main__":
    generate_data()
