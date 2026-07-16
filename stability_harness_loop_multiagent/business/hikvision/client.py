"""Synchronous Hikvision ISAPI HTTP client with Digest Auth (stdlib only).

Sync because TargetAdapter protocol is sync; Worker wraps calls with
asyncio.to_thread for parallelism. Uses urllib.request +
HTTPDigestAuthHandler (no third-party deps), mirroring master branch
tests/agents/device_client.py.
"""

import json
import secrets
import string
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Tuple


class HikvisionClient:
    """Synchronous ISAPI client using stdlib urllib + Digest Auth."""

    def __init__(self, host: str, port: int = 80, username: str = "admin",
                 password: str = "", http_timeout: float = 5.0) -> None:
        host = host.rstrip("/")
        if not host.startswith("http://") and not host.startswith("https://"):
            host = "http://" + host
        self._base = f"{host}:{port}"
        self._user = username
        self._password = password
        self._timeout = http_timeout

        # Digest auth: realm=None lets the handler match any realm.
        pwd_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        pwd_mgr.add_password(None, self._base, self._user, self._password)
        auth_handler = urllib.request.HTTPDigestAuthHandler(pwd_mgr)
        self._opener = urllib.request.build_opener(auth_handler)

    def _url(self, path: str) -> str:
        return self._base + path

    def _request(self, method: str, path: str, body: Any = None,
                 headers: Dict[str, str] = None) -> Tuple[int, bytes]:
        """Send request, return (status_code, response_bytes). Raise on error."""
        url = self._url(path)
        data = None
        req_headers = dict(headers or {})
        if body is not None:
            if isinstance(body, (dict, list)):
                data = json.dumps(body).encode("utf-8")
                req_headers.setdefault("Content-Type", "application/json")
            elif isinstance(body, str):
                data = body.encode("utf-8")
            else:
                data = body
        req = urllib.request.Request(url, data=data, method=method)
        for k, v in req_headers.items():
            req.add_header(k, v)
        try:
            resp = self._opener.open(req, timeout=self._timeout)
            return resp.getcode(), resp.read()
        except urllib.error.HTTPError as e:
            try:
                body_bytes = e.read()
            except Exception:  # noqa: BLE001
                body_bytes = b""
            raise RuntimeError(
                f"HTTP {e.code} on {method} {url}: "
                f"{body_bytes.decode('utf-8', 'replace')}"
            ) from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"URL error on {method} {url}: {e.reason}") from e

    @staticmethod
    def _random_search_id(length: int = 32) -> str:
        """Generate random searchID (Hikvision requires unique per session)."""
        alphabet = string.ascii_letters + string.digits
        return "".join(secrets.choice(alphabet) for _ in range(length))

    def _build_event_cond(self, major: int, minor: int,
                          start: str, end: str,
                          max_results: int = 24) -> Dict[str, Any]:
        return {"AcsEventCond": {
            "searchID": self._random_search_id(),
            "searchResultPosition": 0,
            "maxResults": max_results,
            "major": major,
            "minor": minor,
            "startTime": start,
            "endTime": end,
            "timeReverseOrder": True,
        }}

    def remote_open_door(self, door_no: int = 1) -> Dict[str, Any]:
        """PUT /ISAPI/AccessControl/RemoteOpenDoor/<door>?format=json"""
        path = f"/ISAPI/AccessControl/RemoteOpenDoor/{door_no}?format=json"
        status, body = self._request("PUT", path, body={})
        if status != 200:
            raise RuntimeError(f"remote_open_door returned {status}")
        try:
            return json.loads(body.decode("utf-8")) if body else {}
        except (ValueError, UnicodeDecodeError):
            return {}

    def reboot(self) -> Dict[str, Any]:
        """PUT /ISAPI/System/reboot (returns XML with statusCode)."""
        status, body = self._request(
            "PUT", "/ISAPI/System/reboot", body="",
            headers={"Content-Type": "application/json"})
        if status != 200:
            raise RuntimeError(f"reboot returned {status}")
        return self._parse_status_xml(body)

    @staticmethod
    def _parse_status_xml(xml_bytes: bytes) -> Dict[str, Any]:
        """Parse <ResponseStatus> XML (Hikvision uses xmlns namespace)."""
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as e:
            raise RuntimeError(f"XML parse failed: {e}") from e

        def _local(tag: str) -> str:
            return tag.split("}", 1)[-1] if "}" in tag else tag

        def _find(tag: str):
            for el in root.iter():
                if _local(el.tag) == tag:
                    return el.text
            return None

        return {"statusCode": _find("statusCode"),
                "statusString": _find("statusString")}

    def get_time(self) -> Dict[str, Any]:
        """GET /ISAPI/System/time?format=json"""
        status, body = self._request("GET", "/ISAPI/System/time?format=json")
        if status != 200:
            raise RuntimeError(f"get_time returned {status}")
        return json.loads(body.decode("utf-8"))

    def set_time(self, local_time: str,
                 timezone: str = "CST-8:00") -> Dict[str, Any]:
        """PUT /ISAPI/System/time?format=json"""
        payload = {"Time": {"localTime": local_time, "timeZone": timezone}}
        status, body = self._request(
            "PUT", "/ISAPI/System/time?format=json", body=payload)
        if status != 200:
            raise RuntimeError(f"set_time returned {status}")
        try:
            return json.loads(body.decode("utf-8")) if body else {}
        except (ValueError, UnicodeDecodeError):
            return {}

    def get_work_status(self) -> Dict[str, Any]:
        """GET /ISAPI/AccessControl/AcsWorkStatus?format=json"""
        status, body = self._request(
            "GET", "/ISAPI/AccessControl/AcsWorkStatus?format=json")
        if status != 200:
            raise RuntimeError(f"get_work_status returned {status}")
        return json.loads(body.decode("utf-8"))

    def query_events(self, major: int, minor: int,
                     start: str, end: str) -> List[Dict[str, Any]]:
        """POST /ISAPI/AccessControl/AcsEvent?format=json -> InfoList."""
        payload = self._build_event_cond(major, minor, start, end)
        status, body = self._request(
            "POST", "/ISAPI/AccessControl/AcsEvent?format=json", body=payload)
        if status != 200:
            raise RuntimeError(f"query_events returned {status}")
        data = json.loads(body.decode("utf-8"))
        info_list = data.get("AcsEvent", {}).get("InfoList")
        return info_list or []


__all__ = ["HikvisionClient"]
