import requests
from datetime import datetime, timezone
import uuid

BASE_URL = "http://localhost:8080"

def test_batch_with_internal_duplicate():
    eid = f"evt-batch-dup-{uuid.uuid4()}"

    events = [
        {
            "topic": "batch-dup",
            "event_id": eid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {}
        },
        {
            "topic": "batch-dup",
            "event_id": eid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {}
        }
    ]

    r = requests.post(f"{BASE_URL}/publish", json={"events": events})
    data = r.json()

    assert data["received"] == 2
    assert data["unique_processed"] == 1
    assert data["duplicate_dropped"] == 1
