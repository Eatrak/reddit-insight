import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from kafka import KafkaProducer




def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


KAFKA_BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
print(f"DEBUG: module loaded. Kafka={KAFKA_BOOTSTRAP_SERVERS}", flush=True)
USER_AGENT = _env("REDDIT_USER_AGENT", "trend-insight/0.1")
POLL_INTERVAL_SECONDS = int(_env("REDDIT_POLL_INTERVAL_SECONDS", "15"))
SUBREDDITS = _env("REDDIT_SUBREDDITS", "all")
LIMIT = int(_env("REDDIT_LIMIT", "50"))


def utc_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        linger_ms=50,
        acks="all",
        retries=5,
    )


def reddit_get_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, params=params, timeout=20)
    # 429 happens easily if polling too aggressively
    if r.status_code == 429:
        retry_after = int(r.headers.get("retry-after") or "5")
        time.sleep(retry_after)
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, params=params, timeout=20)
    r.raise_for_status()
    return r.json()



def get_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def extract_post(child: dict[str, Any]) -> Optional[dict[str, Any]]:
    d = child.get("data") or {}
    if not d.get("id"):
        return None
    
    # Strict Schema from README
    return {
        "event_id": d.get("name") or d.get("id"),
        "event_type": "post",
        "subreddit": d.get("subreddit") or "",
        "author": d.get("author") or None,
        "created_utc": utc_iso(float(d.get("created_utc") or time.time())),
        "text": (d.get("title") or "") + "\n" + (d.get("selftext") or ""),
        "score": int(d.get("score") or 0),
        "num_comments": int(d.get("num_comments") or 0),
        "ingested_at": get_now_iso(),
        # Internal fields for debugging only, not strictly required by schema but useful
        # "permalink": d.get("permalink") or "",
        # "url": d.get("url") or "",
    }


def extract_comment(child: dict[str, Any]) -> Optional[dict[str, Any]]:
    d = child.get("data") or {}
    if not d.get("id"):
        return None
        
    return {
        "event_id": d.get("name") or d.get("id"),
        "event_type": "comment",
        "subreddit": d.get("subreddit") or "",
        "author": d.get("author") or None,
        "created_utc": utc_iso(float(d.get("created_utc") or time.time())),
        "text": d.get("body") or "",
        "score": int(d.get("score") or 0),
        "num_comments": 0, # Logic: Comments are leaves in this polling model
        "ingested_at": get_now_iso(),
    }


def poll_loop() -> None:
    print("[ingestion] Initializing producer...", flush=True)
    producer = None
    while producer is None:
        try:
            producer = make_producer()
            print("[ingestion] Producer connected!", flush=True)
        except Exception as e:
            print(f"[ingestion] Failed to connect to Kafka ({e}). Retrying in 5s...", flush=True)
            time.sleep(5)

    last_seen_posts: set[str] = set()
    last_seen_comments: set[str] = set()

    # Sanitized Subreddits
    safe_subreddits = SUBREDDITS.replace(",", "+")
    posts_url = f"https://www.reddit.com/r/{safe_subreddits}/new.json"
    comments_url = f"https://www.reddit.com/r/{safe_subreddits}/comments.json"

    # --- Main Poll Loop ---
    while True:
        try:
            # Poll Posts
            posts = reddit_get_json(posts_url, {"limit": LIMIT})
            for child in (posts.get("data") or {}).get("children") or []:
                msg = extract_post(child)
                if not msg: continue
                if msg["event_id"] in last_seen_posts: continue
                
                producer.send("reddit.raw.posts", key=msg["event_id"], value=msg)
                last_seen_posts.add(msg["event_id"])
            
            # Simple cap
            if len(last_seen_posts) > 10000:
                last_seen_posts = set(list(last_seen_posts)[-5000:])

            # Poll Comments (Backfill not strictly needed for comments as velocity relies mainly on post counts for now)
            comments = reddit_get_json(comments_url, {"limit": LIMIT})
            for child in (comments.get("data") or {}).get("children") or []:
                msg = extract_comment(child)
                if not msg: continue
                if msg["event_id"] in last_seen_comments: continue
                
                producer.send("reddit.raw.comments", key=msg["event_id"], value=msg)
                last_seen_comments.add(msg["event_id"])
            
            if len(last_seen_comments) > 10000:
                last_seen_comments = set(list(last_seen_comments)[-5000:])

            producer.flush(timeout=10)
        except Exception as e:  # noqa: BLE001
            print(f"[ingestion] error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)





# -----------------------------------------------------------------------------
# Backfill Worker
# -----------------------------------------------------------------------------
def backfill_task_consumer():
    """
    Listens for backfill tasks on 'reddit.tasks.backfill'.
    Payload: { topic_id: str, subreddits: List[str] }
    """
    print("[worker] Starting backfill consumer thread...", flush=True)
    
    # Needs a separate consumer group
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        "reddit.tasks.backfill",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id="ingestion-backfill-worker",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest"
    )

    producer = make_producer() # Dedicated producer for worker

    for message in consumer:
        try:
            task = message.value
            topic_id = task.get("topic_id")
            subreddits = task.get("subreddits", [])
            
            print(f"[worker] Received backfill task for topic={topic_id} subs={len(subreddits)}", flush=True)
            
            # Execute Backfill
            perform_backfill(topic_id, subreddits, producer)
            
        except Exception as e:
            print(f"[worker] Error processing task: {e}", flush=True)


def perform_backfill(topic_id: str, subreddits: list[str], producer: KafkaProducer):
    """
    Fetches 7 days history for specific subreddits and pushes to reddit.raw.posts.
    Updates API status to COMPLETED upon finish.
    """
    if not subreddits:
        update_api_status(topic_id, "COMPLETED")
        return

    print(f"[worker] Starting processing for {topic_id}...", flush=True)
    cutoff = time.time() - (7 * 24 * 3600)
    
    # We process subreddits sequentially to avoid rate limits per token (though we use same token/IP)
    # Better to chunk? 
    # For now, let's treat them as a combined multireddit "r/sub1+sub2" to be efficient
    # Reddit allows max ~100 subs in one query.
    
    chunk_size = 50
    chunks = [subreddits[i:i + chunk_size] for i in range(0, len(subreddits), chunk_size)]
    
    for i, chunk in enumerate(chunks):
        subs_str = "+".join([s.strip() for s in chunk if s.strip()])
        if not subs_str: continue

        print(f"[worker] Fetching history for chunk {i+1}/{len(chunks)}: {subs_str[:50]}...", flush=True)
        
        url = f"https://www.reddit.com/r/{subs_str}/new.json"
        
        after = None
        keep_fetching = True
        fetched = 0
        
        while keep_fetching:
            try:
                params = {"limit": 100}
                if after: params["after"] = after
                
                data = reddit_get_json(url, params)
                children = (data.get("data") or {}).get("children") or []
                
                if not children:
                    break
                
                for child in children:
                    msg = extract_post(child)
                    if not msg: continue
                    
                    ts = float(child["data"].get("created_utc") or 0)
                    if ts < cutoff:
                        keep_fetching = False
                        break
                    
                    # Push to Kafka with reference to topic_id if needed? 
                    # No, Spark matches by keyword. As long as we ingest the raw post, Spark picks it up.
                    producer.send("reddit.raw.posts", key=msg["event_id"], value=msg)
                    
                    after = child["data"]["name"]
                
                fetched += len(children)
                if fetched > 5000: # Safety cap per chunk
                    print(f"[worker] Chunk safety cap reached ({fetched})")
                    break
                    
                time.sleep(1.5) # Respect rate limits
                
            except Exception as e:
                print(f"[worker] Chunk error: {e}")
                break
    
    print(f"[worker] Backfill for {topic_id} complete.", flush=True)
    update_api_status(topic_id, "COMPLETED")


def update_api_status(topic_id: str, status: str):
    try:
        api_url = f"http://trend-api:8000/topics/{topic_id}/status" # Internal docker DNS
        requests.patch(api_url, json={"status": status}, timeout=5)
    except Exception as e:
        print(f"[worker] Failed to update status for {topic_id}: {e}")


if __name__ == "__main__":
    print(f"[ingestion] kafka={KAFKA_BOOTSTRAP_SERVERS} subreddits={SUBREDDITS} interval={POLL_INTERVAL_SECONDS}s")

    # Start Background Consumer for Backfill Tasks
    import threading
    t = threading.Thread(target=backfill_task_consumer, daemon=True)
    t.start()

    poll_loop()
