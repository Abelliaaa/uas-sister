import requests

BASE_URL = "http://localhost:8080"

def test_invalid_timestamp():
    payload = {
        "events": [
            {
                "topic": "invalid-time",
                "event_id": "evt-invalid-time",
                "timestamp": "NOT-A-TIME",
                "source": "pytest",
                "payload": {}
            }
        ]
    }

    r = requests.post(f"{BASE_URL}/publish", json=payload)
    assert r.status_code == 422
