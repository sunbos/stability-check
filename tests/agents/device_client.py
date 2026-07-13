"""海康 ISAPI 访问封装（仅使用标准库 urllib / json / xml / os）。

提供 DeviceClient 用于：
  - reboot()                远程重启设备
  - get_work_status()       获取门禁工作状态码（JSON）
  - get_reboot_events()     查询指定时间窗口内的 AcsEvent 事件

HTTP Digest 认证基于 urllib.request.HTTPDigestAuthHandler + HTTPPasswordMgrWithDefaultRealm。
所有请求统一 timeout=20 秒。
"""

import json
import os
import secrets
import string
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET


# 默认关注的状态字段（用于基线对比）
DEFAULT_FIELDS = [
    "doorLockStatus",
    "doorStatus",
    "wifiStatus",
    "magneticStatus",
    "cardReaderOnlineStatus",
    "doorOnlineStatus",
]


class DeviceClient:
    """封装海康 ISAPI 的 HTTP Digest 访问。"""

    def __init__(self, host: str, user: str, password: str):
        host = host.rstrip("/")
        if not host.startswith("http://") and not host.startswith("https://"):
            host = "http://" + host
        self.host = host
        self.user = user
        self.password = password

        # Digest 认证：realm 传 None 即可
        pwd_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        pwd_mgr.add_password(None, self.host, self.user, self.password)
        auth_handler = urllib.request.HTTPDigestAuthHandler(pwd_mgr)
        self._opener = urllib.request.build_opener(auth_handler)

    def _request(self, method: str, path: str, body=None, headers=None):
        """发送请求并返回 (status_code, response_bytes)。失败时抛异常。"""
        url = self.host + path
        data = None
        req_headers = {}
        if headers:
            req_headers.update(headers)
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
            resp = self._opener.open(req, timeout=20)
            return resp.getcode(), resp.read()
        except urllib.error.HTTPError as e:
            # 仍取出响应体，便于上层解析错误信息
            try:
                body_bytes = e.read()
            except Exception:
                body_bytes = b""
            raise RuntimeError(
                f"HTTP错误 {e.code} 于 {method} {url}: {body_bytes.decode('utf-8', 'replace')}"
            ) from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"URL错误 于 {method} {url}: {e.reason}") from e
        except Exception as e:
            raise RuntimeError(f"请求失败 于 {method} {url}: {e}") from e

    @staticmethod
    def _parse_status_xml(xml_bytes: bytes) -> dict:
        """解析 ISAPI 返回的 <ResponseStatus> XML，返回 dict。

        注意响应带命名空间（xmlns="http://www.hikvision.com/ver10/XMLSchema"），
        因此按本地标签名（忽略命名空间）查找，避免 root.find 匹配不到。
        """
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as e:
            raise RuntimeError(f"解析XML响应失败: {e}") from e

        def _local(tag: str) -> str:
            return tag.split("}", 1)[-1] if "}" in tag else tag

        def _find(tag: str):
            for el in root.iter():
                if _local(el.tag) == tag:
                    return el.text
            return None

        return {
            "statusCode": _find("statusCode"),
            "statusString": _find("statusString"),
        }

    def reboot(self) -> dict:
        """发送 PUT /ISAPI/System/reboot，返回含 statusCode/statusString 的 dict。"""
        status, body = self._request(
            "PUT",
            "/ISAPI/System/reboot",
            body="",
            headers={"Content-Type": "application/json"},
        )
        if status != 200:
            raise RuntimeError(f"重启返回状态码 {status}")
        result = self._parse_status_xml(body)
        if result.get("statusCode") is None:
            raise RuntimeError(f"重启响应缺少 statusCode: {body!r}")
        return result

    def get_work_status(self) -> dict:
        """GET /ISAPI/AccessControl/AcsWorkStatus?format=json，返回 dict。"""
        status, body = self._request(
            "GET",
            "/ISAPI/AccessControl/AcsWorkStatus?format=json",
        )
        if status != 200:
            raise RuntimeError(f"获取工作状态返回状态码 {status}")
        try:
            return json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            raise RuntimeError(f"解析工作状态 JSON 失败: {e}") from e

    @staticmethod
    def _random_search_id(length: int = 32) -> str:
        """生成随机 searchID：length 位大小写字母+数字，每次调用都不同。

        海康 ISAPI 要求 searchID 为随机串，且同一会话内不应重复，否则可能命中缓存。
        """
        alphabet = string.ascii_letters + string.digits
        return "".join(secrets.choice(alphabet) for _ in range(length))

    def get_reboot_events(
        self,
        start: str,
        end: str,
        major: int = 3,
        minor: int = 123,
        limit: int = 10,
        search_id: str | None = None,
    ) -> list:
        """POST /ISAPI/AccessControl/AcsEvent?format=json，返回 InfoList。

        start/end 必须为 'YYYY-MM-DDTHH:MM:SS' 格式（不带空格+时区）。
        searchID 每次调用随机生成（可用 search_id 覆盖，便于测试）。
        """
        cond = {
            "AcsEventCond": {
                "searchID": search_id or self._random_search_id(),
                "searchResultPosition": 0,
                "maxResults": limit,
                "major": major,
                "minor": minor,
                "startTime": start,
                "endTime": end,
                "timeReverseOrder": True,
            }
        }
        status, body = self._request(
            "POST",
            "/ISAPI/AccessControl/AcsEvent?format=json",
            body=cond,
        )
        if status != 200:
            raise RuntimeError(f"获取重启事件返回状态码 {status}")
        try:
            data = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            raise RuntimeError(f"解析 AcsEvent JSON 失败: {e}") from e

        info_list = data.get("AcsEvent", {}).get("InfoList")
        if not info_list:
            return []
        return info_list
