import requests
from datetime import datetime, timezone
import uuid

BASE_URL = "http://localhost:8080"

def test_batch_publish():
    events = []
    for i in range(10):
        events.append({
            "topic": "batch",
            "event_id": f"evt-batch-{uuid.uuid4()}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {"i": i}
        })

    r = requests.post(f"{BASE_URL}/publish", json={"events": events})
    assert r.status_code == 200

    data = r.json()
    assert data["received"] == 10
    assert data["unique_processed"] == 10
