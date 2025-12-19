import requests

BASE_URL = "http://localhost:8080"

def test_invalid_schema_missing_event_id():
    payload = {
        "events": [
            {
                "topic": "invalid",
                "timestamp": "2025-01-01T00:00:00Z",
                "source": "pytest",
                "payload": {}
            }
        ]
    }

    r = requests.post(f"{BASE_URL}/publish", json=payload)
    assert r.status_code == 422
