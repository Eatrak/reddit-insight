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
USER_AGENT = _env("REDDIT_USER_AGENT", "reddit-insight/0.1")
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


def extract_post(child: dict[str, Any]) -> Optional[dict[str, Any]]:
    d = child.get("data") or {}
    if not d.get("id"):
        return None
    return {
        "id": d.get("name") or d.get("id"),
        "subreddit": d.get("subreddit") or "",
        "author": d.get("author") or "",
        "created_utc": utc_iso(float(d.get("created_utc") or time.time())),
        "score": int(d.get("score") or 0),
        "text": (d.get("title") or "") + "\n" + (d.get("selftext") or ""),
        "type": "post",
        "permalink": d.get("permalink") or "",
        "url": d.get("url") or "",
    }


def extract_comment(child: dict[str, Any]) -> Optional[dict[str, Any]]:
    d = child.get("data") or {}
    if not d.get("id"):
        return None
    return {
        "id": d.get("name") or d.get("id"),
        "subreddit": d.get("subreddit") or "",
        "author": d.get("author") or "",
        "created_utc": utc_iso(float(d.get("created_utc") or time.time())),
        "score": int(d.get("score") or 0),
        "text": d.get("body") or "",
        "type": "comment",
        "permalink": d.get("permalink") or "",
        "link_id": d.get("link_id") or "",
    }


def poll_loop() -> None:
    producer = make_producer()
    last_seen_posts: set[str] = set()
    last_seen_comments: set[str] = set()

    posts_url = f"https://www.reddit.com/r/{SUBREDDITS}/new.json"
    comments_url = f"https://www.reddit.com/r/{SUBREDDITS}/comments.json"

    while True:
        try:
            posts = reddit_get_json(posts_url, {"limit": LIMIT})
            for child in (posts.get("data") or {}).get("children") or []:
                msg = extract_post(child)
                if not msg:
                    continue
                if msg["id"] in last_seen_posts:
                    continue
                producer.send("reddit.posts.raw", key=msg["id"], value=msg)
                last_seen_posts.add(msg["id"])
            # cap memory
            if len(last_seen_posts) > 5000:
                last_seen_posts = set(list(last_seen_posts)[-2000:])

            comments = reddit_get_json(comments_url, {"limit": LIMIT})
            for child in (comments.get("data") or {}).get("children") or []:
                msg = extract_comment(child)
                if not msg:
                    continue
                if msg["id"] in last_seen_comments:
                    continue
                producer.send("reddit.comments.raw", key=msg["id"], value=msg)
                last_seen_comments.add(msg["id"])
            if len(last_seen_comments) > 5000:
                last_seen_comments = set(list(last_seen_comments)[-2000:])

            producer.flush(timeout=10)
        except Exception as e:  # noqa: BLE001
            print(f"[ingestion] error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    print(f"[ingestion] kafka={KAFKA_BOOTSTRAP_SERVERS} subreddits={SUBREDDITS} interval={POLL_INTERVAL_SECONDS}s")
    poll_loop()

