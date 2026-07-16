# tests/test_hikvision_event_codes.py
from stability_harness_loop_multiagent.business.hikvision.event_codes import HikEventCode


def test_remote_open_event_code():
    assert HikEventCode.REMOTE_OPEN == (3, 1024)


def test_lock_open_event_code():
    assert HikEventCode.LOCK_OPEN == (5, 21)


def test_lock_close_event_code():
    assert HikEventCode.LOCK_CLOSE == (5, 22)


def test_face_pass_event_code():
    assert HikEventCode.FACE_PASS == (5, 75)


def test_event_code_is_major_minor_tuple():
    for code in [HikEventCode.REMOTE_OPEN, HikEventCode.LOCK_OPEN,
                 HikEventCode.LOCK_CLOSE, HikEventCode.FACE_PASS]:
        assert isinstance(code, tuple) and len(code) == 2
        major, minor = code
        assert isinstance(major, int) and isinstance(minor, int)
