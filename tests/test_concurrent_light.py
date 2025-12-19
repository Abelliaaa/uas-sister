import requests
from datetime import datetime, timezone
import uuid
import threading

BASE_URL = "http://localhost:8080"

def test_light_concurrent_publish():
    eid = f"evt-concurrent-light-{uuid.uuid4()}"

    payload = {
        "events": [
            {
                "topic": "concurrent",
                "event_id": eid,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "pytest",
                "payload": {}
            }
        ]
    }

    responses = []

    def send():
        r = requests.post(f"{BASE_URL}/publish", json=payload)
        responses.append(r.json())

    t1 = threading.Thread(target=send)
    t2 = threading.Thread(target=send)

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    uniques = sum(r["unique_processed"] for r in responses)
    assert uniques == 1
