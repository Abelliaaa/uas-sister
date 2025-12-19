from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
from time import time
from typing import Dict, Any, List
import psycopg2
import os
import json
import threading
import time as _time

from psycopg2.extras import Json
import redis

# =================
# CONFIG
# =================

DATABASE_URL = os.getenv("DATABASE_URL")
BROKER_URL = os.getenv("BROKER_URL")

ASYNC_MODE = os.getenv("ASYNC_MODE", "false").lower() == "true"
QUEUE_NAME = "event_queue"

app = FastAPI(
    title="Pub-Sub Log Aggregator",
    version="1.0.0"
)

START_TIME = time()

# Redis client
rds = redis.from_url(BROKER_URL, decode_responses=True) if ASYNC_MODE else None

# =================
# MODELS
# =================

class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: Dict[str, Any]

class EventBatch(BaseModel):
    events: List[Event]

# =================
# DB HELPER
# =================

def get_conn(retries=10, delay=1):
    for i in range(retries):
        try:
            return psycopg2.connect(DATABASE_URL)
        except psycopg2.OperationalError:
            if i == retries - 1:
                raise
            _time.sleep(delay)

# =================
# WORKER (ASYNC MODE)
# =================

def worker():
    while True:
        _, raw = rds.blpop(QUEUE_NAME)
        event = json.loads(raw)

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # insert + dedup
                    cur.execute(
                        """
                        INSERT INTO processed_events
                        (topic, event_id, timestamp, source, payload)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (topic, event_id) DO NOTHING
                        """,
                        (
                            event["topic"],
                            event["event_id"],
                            event["timestamp"],
                            event["source"],
                            Json(event["payload"]),
                        )
                    )

                    if cur.rowcount == 1:
                        cur.execute("""
                            UPDATE stats_counter
                            SET unique_processed = unique_processed + 1
                            WHERE id = 1
                        """)
                    else:
                        cur.execute("""
                            UPDATE stats_counter
                            SET duplicate_dropped = duplicate_dropped + 1
                            WHERE id = 1
                        """)

        except Exception as e:
            print("Worker error:", e)

@app.on_event("startup")
def start_worker():
    if ASYNC_MODE:
        threading.Thread(target=worker, daemon=True).start()

# =================
# API
# =================

@app.post("/publish")
def publish(batch: EventBatch):
    # === ASYNC MODE ===
    if ASYNC_MODE:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE stats_counter
                    SET received = received + %s
                    WHERE id = 1
                """, (len(batch.events),))

        for event in batch.events:
            rds.rpush(QUEUE_NAME, event.model_dump_json())

        return {
            "status": "queued",
            "received": len(batch.events),
        }

    # === SYNC MODE ===
    processed = 0
    duplicates = 0

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                for event in batch.events:
                    cur.execute(
                        """
                        INSERT INTO processed_events
                        (topic, event_id, timestamp, source, payload)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (topic, event_id) DO NOTHING
                        """,
                        (
                            event.topic,
                            event.event_id,
                            event.timestamp,
                            event.source,
                            Json(event.payload),
                        )
                    )

                    if cur.rowcount == 1:
                        processed += 1
                    else:
                        duplicates += 1

                cur.execute("""
                    UPDATE stats_counter
                    SET
                        received = received + %s,
                        unique_processed = unique_processed + %s,
                        duplicate_dropped = duplicate_dropped + %s
                    WHERE id = 1
                """, (len(batch.events), processed, duplicates))

        return {
            "status": "processed",
            "received": len(batch.events),
            "unique_processed": processed,
            "duplicate_dropped": duplicates,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events")
def get_events(topic: str):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT topic, event_id, timestamp, source, payload
                    FROM processed_events
                    WHERE topic = %s
                    ORDER BY created_at
                    """,
                    (topic,)
                )
                rows = cur.fetchall()

        return [
            {
                "topic": r[0],
                "event_id": r[1],
                "timestamp": r[2],
                "source": r[3],
                "payload": r[4],
            }
            for r in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
def stats():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT received, unique_processed, duplicate_dropped
                    FROM stats_counter
                    WHERE id = 1
                """)
                r, u, d = cur.fetchone()

        return {
            "received": r,
            "unique_processed": u,
            "duplicate_dropped": d,
            "uptime_seconds": int(time() - START_TIME),
            "mode": "async" if ASYNC_MODE else "sync"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
