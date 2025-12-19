import requests

BASE_URL = "http://localhost:8080"

def test_get_events_unique():
    r = requests.get(f"{BASE_URL}/events", params={"topic": "dedup"})
    assert r.status_code == 200

    events = r.json()
    event_ids = [e["event_id"] for e in events]

    # pastikan tidak ada duplikasi
    assert len(event_ids) == len(set(event_ids))
