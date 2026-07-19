# tests/test_hikvision_client.py
from stability_harness_loop_multiagent.business.hikvision.client import HikvisionClient


def test_client_builds_isapi_url():
    client = HikvisionClient("192.168.3.33", 80, "admin", "pass")
    assert client._url("/ISAPI/System/time") == "http://192.168.3.33:80/ISAPI/System/time"


def test_client_builds_query_events_payload():
    client = HikvisionClient("192.168.3.33", 80, "admin", "pass")
    payload = client._build_event_cond(
        major=3, minor=1024,
        start="2026-07-17T03:20:00+08:00",
        end="2026-07-17T03:25:00+08:00",
    )
    assert payload["AcsEventCond"]["major"] == 3
    assert payload["AcsEventCond"]["minor"] == 1024
    assert payload["AcsEventCond"]["startTime"] == "2026-07-17T03:20:00+08:00"
    assert "searchID" in payload["AcsEventCond"]
    assert payload["AcsEventCond"]["timeReverseOrder"] is True


def test_client_random_search_id_is_unique():
    client = HikvisionClient("192.168.3.33", 80, "admin", "pass")
    a = client._random_search_id()
    b = client._random_search_id()
    assert len(a) == 32 and len(b) == 32
    assert a != b  # Hikvision requires unique searchID per session

