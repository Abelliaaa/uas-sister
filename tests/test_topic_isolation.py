import requests
from datetime import datetime, timezone
import uuid

BASE_URL = "http://localhost:8080"

def test_same_event_id_different_topic():
    eid = f"evt-topic-{uuid.uuid4()}"

    e1 = {
        "topic": "topic-a",
        "event_id": eid,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "pytest",
        "payload": {}
    }

    e2 = {
        "topic": "topic-b",
        "event_id": eid,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "pytest",
        "payload": {}
    }

    r = requests.post(f"{BASE_URL}/publish", json={"events": [e1, e2]})
    data = r.json()

    assert data["unique_processed"] == 2
