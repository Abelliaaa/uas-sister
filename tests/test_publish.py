import requests
from datetime import datetime, timezone
import uuid

BASE_URL = "http://localhost:8080"

def test_publish_single_event():
    event_id = f"evt-publish-{uuid.uuid4()}"

    payload = {
        "events": [
            {
                "topic": "test",
                "event_id": event_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "pytest",
                "payload": {"msg": "hello"}
            }
        ]
    }

    r = requests.post(f"{BASE_URL}/publish", json=payload)
    assert r.status_code == 200

    data = r.json()
    assert data["received"] == 1
    assert data["unique_processed"] == 1
    assert data["duplicate_dropped"] == 0
