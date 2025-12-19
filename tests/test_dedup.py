import requests
from datetime import datetime, timezone
import uuid

BASE_URL = "http://localhost:8080"

def test_deduplication():
    event_id = f"evt-dedup-{uuid.uuid4()}"

    payload = {
        "events": [
            {
                "topic": "dedup",
                "event_id": event_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "pytest",
                "payload": {"msg": "dup"}
            }
        ]
    }

    r1 = requests.post(f"{BASE_URL}/publish", json=payload)
    r2 = requests.post(f"{BASE_URL}/publish", json=payload)

    assert r1.status_code == 200
    assert r2.status_code == 200

    d1 = r1.json()
    d2 = r2.json()

    assert d1["unique_processed"] == 1
    assert d2["duplicate_dropped"] == 1
