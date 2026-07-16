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


# --- FakeHikvisionClient tests (Task 4) ---
from tests.fakes.fake_hikvision import FakeHikvisionClient


def test_fake_client_records_remote_open_and_returns_events():
    fake = FakeHikvisionClient()
    fake.remote_open_door(door_no=1)
    # Query remote-open event (major=3, minor=1024)
    evs = fake.query_events(3, 1024, fake._win_start, fake._win_end)
    assert len(evs) == 1
    assert evs[0]["major"] == 3 and evs[0]["minor"] == 1024
    # Query lock-open event (major=5, minor=21)
    opens = fake.query_events(5, 21, fake._win_start, fake._win_end)
    assert len(opens) == 1
    # Query lock-close event (major=5, minor=22)
    closes = fake.query_events(5, 22, fake._win_start, fake._win_end)
    assert len(closes) == 1


def test_fake_client_time_skew():
    fake = FakeHikvisionClient(time_skew_seconds=10.0)
    t = fake.get_time()
    # Device time differs from host by skew
    assert "localTime" in t["Time"]

