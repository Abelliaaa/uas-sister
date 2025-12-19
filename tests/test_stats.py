import requests

BASE_URL = "http://localhost:8080"

def test_stats_consistency():
    r = requests.get(f"{BASE_URL}/stats")
    assert r.status_code == 200

    stats = r.json()

    assert stats["received"] >= stats["unique_processed"]
    assert stats["received"] >= stats["duplicate_dropped"]
