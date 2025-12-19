import requests
from datetime import datetime, timezone
import uuid

BASE_URL = "http://localhost:8080"

def test_stats_increase_after_publish():
    before = requests.get(f"{BASE_URL}/stats").json()["unique_processed"]

    payload = {
        "events": [
            {
                "topic": "stats",
                "event_id": f"evt-stats-{uuid.uuid4()}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "pytest",
                "payload": {}
            }
        ]
    }

    requests.post(f"{BASE_URL}/publish", json=payload)

    after = requests.get(f"{BASE_URL}/stats").json()["unique_processed"]
    assert after == before + 1
