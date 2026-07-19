"""海康 ISAPI HTTP 同步客户端，使用 Digest 鉴权（httpx + httpx.DigestAuth）。

采用同步方式，因为 TargetAdapter 协议本身是同步的；Worker 通过
asyncio.to_thread 包裹调用以实现并行。底层使用 httpx.Client +
httpx.DigestAuth 替换原手写 urllib + HTTPDigestAuthHandler，
对应主干分支的 tests/agents/device_client.py。

注：原计划用 httpx_auth.DigestAuth，但 httpx_auth 0.23.x 不提供 Digest
（仅 Basic / API key / OAuth2 / AWS4 等）。httpx 自带 ``httpx.DigestAuth``，
是官方推荐的 Digest 鉴权实现，故改用之。属于 httpx 生态，仍满足
"用 httpx 生态替换手写 urllib" 的目标。
"""

import json
import re
import secrets
import string
import threading
import time
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Tuple

import httpx

# 海康 RemoteControl/door 接口要求 XML 报文（在该固件上 JSON 变体会
# 返回 "notSupport"）。xmlns 命名空间是必填项。
_REMOTE_OPEN_XML = (
    '<RemoteControlDoor version="2.0" '
    'xmlns="http://www.isapi.org/ver20/XMLSchema">'
    "<cmd>open</cmd>"
    "</RemoteControlDoor>"
)

# 去掉 ISO 8601 时间戳中的微秒部分。海康会拒绝
# "2026-07-17T03:29:00.978924+08:00"（报 badJsonContent），仅接受
# "2026-07-17T03:29:00+08:00"。
_MICRO_SEC_RE = re.compile(r"\.\d+(?=[+\-Z]|$)")


def _strip_microseconds(ts: str) -> str:
    """去掉 ISO 8601 时间戳中的小数秒部分。"""
    return _MICRO_SEC_RE.sub("", ts)


class HikvisionClient:
    """使用 httpx + httpx.DigestAuth 的同步 ISAPI 客户端。"""

    def __init__(self, host: str, port: int = 80, username: str = "admin",
                 password: str = "", http_timeout: float = 5.0,
                 timeout: float | None = None) -> None:
        # 兼容两种关键字：原版用 ``http_timeout``，TDD 测试用 ``timeout``。
        # 若显式传入 ``timeout`` 则优先使用，否则回退到 ``http_timeout``。
        effective_timeout = timeout if timeout is not None else http_timeout
        host = host.rstrip("/")
        if not host.startswith("http://") and not host.startswith("https://"):
            host = "http://" + host
        base_url = f"{host}:{port}"

        self._client = httpx.Client(
            base_url=base_url,
            timeout=httpx.Timeout(effective_timeout),
        )
        self._auth = httpx.DigestAuth(username, password)
        self._timeout = effective_timeout
        self._base = base_url

        # httpx.DigestAuth 在多线程并发请求时会共享 nonce/nc 状态，
        # Worker 通过 asyncio.to_thread 并行调用 query_events；若无锁保护，
        # 并发请求会破坏鉴权状态（导致 HTTP 401）。
        self._lock = threading.Lock()

    def _url(self, path: str) -> str:
        """拼接完整 URL（仅用于错误信息可读性，httpx 调用本身用 base_url + path）。"""
        return self._base + path

    def _request(self, method: str, path: str, body: Any = None,
                 headers: Dict[str, str] = None, retries: int = 2) -> Tuple[int, bytes]:
        """发送请求，返回 (状态码, 响应字节)。出错时抛出异常。

        通过 self._lock 串行化，因为 httpx.DigestAuth 的 nonce/nc
        状态并非线程安全（并发调用会导致 HTTP 401）。

        网络层错误（``httpx.TransportError``，含连接超时 / 拒绝 / 握手失败）
        按指数退避重试 ``retries`` 次（默认 2，即最多 3 次尝试），缓解真实设备
        瞬时抖动（首次运行偶发的 ``timed out``）；HTTP 状态错误
        （4xx/5xx）属于确定性失败，**不**重试，直接抛 RuntimeError 带响应体。
        """
        url = self._url(path)
        req_headers = dict(headers or {})
        content: bytes | None = None
        if body is not None:
            if isinstance(body, (dict, list)):
                content = json.dumps(body).encode("utf-8")
                req_headers.setdefault("Content-Type", "application/json")
            elif isinstance(body, str):
                content = body.encode("utf-8")
            else:
                content = body

        last_exc: Exception | None = None
        for attempt in range(retries + 1):
            with self._lock:
                try:
                    resp = self._client.request(
                        method, path,
                        content=content,
                        headers=req_headers,
                        auth=self._auth,
                    )
                except httpx.TransportError as e:
                    # 网络层错误（连接超时/拒绝/握手失败）：重试。
                    last_exc = e
                    if attempt < retries:
                        # 指数退避：0.5s、1.0s（封顶 3.0s），避免打爆设备。
                        time.sleep(min(0.5 * (2 ** attempt), 3.0))
                        continue
                    raise RuntimeError(f"Transport error on {method} {url}: {e}") from e
                # httpx 默认对 4xx/5xx 不抛异常（需显式 raise_for_status），
                # 这里手动检查并抛 RuntimeError 带响应体，保持与原 urllib 行为一致。
                if resp.status_code >= 400:
                    body_bytes = resp.content or b""
                    raise RuntimeError(
                        f"HTTP {resp.status_code} on {method} {url}: "
                        f"{body_bytes.decode('utf-8', 'replace')}"
                    )
                return resp.status_code, resp.content
        # 兜底（retries>=0 时循环内必已 raise；此处理论上不可达）。
        raise RuntimeError(f"Transport error on {method} {url}: {last_exc}") from last_exc

    def request_json(self, method: str, path: str, body: Any = None,
                      headers: Dict[str, str] = None) -> Any:
        """通用的「请求 -> 解析 JSON」便捷方法（供场景化适配器等复用）。

        在 ``_request`` 之上封装：非 200 抛 ``RuntimeError``；200 时尝试把响应体
        解析为 JSON。解析失败（如设备返回 XML）时返回 ``{"_raw": <文本>}``，
        调用方可据此做基于文本的兜底处理。这是**增量、非破坏性**的通用入口，
        不改动任何既有方法的行为。
        """
        status, raw = self._request(method, path, body, headers)
        if status != 200:
            raise RuntimeError(f"{method} {path} returned {status}")
        text = raw.decode("utf-8", "replace").strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"_raw": text}

    @staticmethod
    def _random_search_id(length: int = 32) -> str:
        """生成随机 searchID（海康要求每个会话唯一）。"""
        alphabet = string.ascii_letters + string.digits
        return "".join(secrets.choice(alphabet) for _ in range(length))

    def _build_event_cond(self, major: int, minor: int,
                          start: str, end: str,
                          max_results: int = 24) -> Dict[str, Any]:
        # 海康会拒绝带小数秒的时间戳，先去掉。
        return {"AcsEventCond": {
            "searchID": self._random_search_id(),
            "searchResultPosition": 0,
            "maxResults": max_results,
            "major": major,
            "minor": minor,
            "startTime": _strip_microseconds(start),
            "endTime": _strip_microseconds(end),
            "timeReverseOrder": True,
        }}

    def remote_open_door(self, door_no: int = 1) -> Dict[str, Any]:
        """PUT /ISAPI/AccessControl/RemoteControl/door/<door>，使用 XML 报文。

        JSON 变体 /RemoteOpenDoor/<door>?format=json 在测试固件上返回
        "notSupport"；XML 的 RemoteControl 接口是官方文档支持且可用的路径
        （已在 DS-K1T502 上验证）。
        """
        path = f"/ISAPI/AccessControl/RemoteControl/door/{door_no}"
        status, body = self._request(
            "PUT", path, body=_REMOTE_OPEN_XML.encode("utf-8"),
            headers={"Content-Type": "application/xml"})
        if status != 200:
            raise RuntimeError(f"remote_open_door returned {status}")
        return self._parse_status_xml(body)

    def reboot(self) -> Dict[str, Any]:
        """PUT /ISAPI/System/reboot（返回含 statusCode 的 XML）。"""
        status, body = self._request(
            "PUT", "/ISAPI/System/reboot", body="",
            headers={"Content-Type": "application/json"})
        if status != 200:
            raise RuntimeError(f"reboot returned {status}")
        return self._parse_status_xml(body)

    @staticmethod
    def _parse_status_xml(xml_bytes: bytes) -> Dict[str, Any]:
        """解析 <ResponseStatus> XML（海康使用 xmlns 命名空间）。

        返回 statusCode（尽量转 int，便于与 7/1 等常量比较）、statusString、
        subStatusCode（如 ``autoReboot``）、errorMsg。串口配置切换在返回
        statusCode=7 / subStatusCode=autoReboot 时表示设备将自动重启。
        """
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

        raw_sc = _find("statusCode")
        try:
            status_code = int(raw_sc) if raw_sc is not None else None
        except ValueError:
            status_code = raw_sc
        return {"statusCode": status_code,
                "statusString": _find("statusString"),
                "subStatusCode": _find("subStatusCode"),
                "errorMsg": _find("errorMsg")}

    # ---- 串口外设类型（Serial Port）API -------------------------------
    # 用于前置条件就绪检查：门离线可能是串口 1 的外设类型(mode)不对，需切换。
    def get_serial_capabilities(self, port: int = 1) -> Dict[str, Any]:
        """GET /ISAPI/System/Serial/ports/<port>/capabilities。

        返回各字段的可选值（``opt`` 属性）。例如 ``mode`` ->
        ``["readerMode", "externMode", "accessControlHost", "accessDetection"]``。
        """
        status, body = self._request(
            "GET", f"/ISAPI/System/Serial/ports/{port}/capabilities")
        if status != 200:
            raise RuntimeError(f"get_serial_capabilities returned {status}")
        return self._parse_serial_xml(body)

    def get_serial_config(self, port: int = 1) -> Dict[str, Any]:
        """GET /ISAPI/System/Serial/ports/<port>。

        返回当前串口配置（含 ``mode`` 等字段）。
        """
        status, body = self._request(
            "GET", f"/ISAPI/System/Serial/ports/{port}")
        if status != 200:
            raise RuntimeError(f"get_serial_config returned {status}")
        return self._parse_serial_xml(body)

    def set_serial_config(self, port: int,
                          fields: Dict[str, str]) -> Dict[str, Any]:
        """PUT /ISAPI/System/Serial/ports/<port>。

        ``fields`` 为完整 ``SerialPort`` 字段字典（id/mode/deviceName/...）。
        回写完整配置、仅替换 ``mode`` 即可切换外设类型。返回解析后的
        ResponseStatus 字典；当设备要求自动重启时 ``subStatusCode=autoReboot``
        （statusCode=7），调用方须等待设备重启并重新上线。
        """
        inner = "".join(f"<{k}>{v}</{k}>" for k, v in fields.items())
        xml_body = (
            '<SerialPort version="2.0" '
            'xmlns="http://www.isapi.org/ver20/XMLSchema">'
            f"{inner}</SerialPort>"
        ).encode("utf-8")
        status, body = self._request(
            "PUT", f"/ISAPI/System/Serial/ports/{port}", body=xml_body,
            headers={"Content-Type": "application/xml"})
        if status != 200:
            raise RuntimeError(f"set_serial_config returned {status}")
        return self._parse_status_xml(body)

    @staticmethod
    def _parse_serial_xml(xml_bytes: bytes) -> Dict[str, Any]:
        """解析 <SerialPort> 下所有子元素。

        对带 ``opt`` 属性的字段（如 capabilities 的 mode/baudRate），取值为
        opt 逗号分隔列表；其余取文本内容。
        """
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as e:
            raise RuntimeError(f"XML parse failed: {e}") from e

        def _local(tag: str) -> str:
            return tag.split("}", 1)[-1] if "}" in tag else tag

        out: Dict[str, Any] = {}
        for el in root.iter():
            tag = _local(el.tag)
            if tag == "SerialPort":
                continue
            opt = el.get("opt")
            if opt is not None:
                out[tag] = [x.strip() for x in opt.split(",") if x.strip()]
            else:
                out[tag] = el.text
        return out

    def get_time(self) -> Dict[str, Any]:
        """GET /ISAPI/System/time -> 解析 XML（固件忽略 ?format=json）。

        返回 ``{"Time": {"localTime": ..., "timeZone": ...}}``，以匹配
        worker.py / 假客户端所用的 JSON 形态契约。
        """
        status, body = self._request("GET", "/ISAPI/System/time")
        if status != 200:
            raise RuntimeError(f"get_time returned {status}")
        return self._parse_time_xml(body)

    @staticmethod
    def _parse_time_xml(xml_bytes: bytes) -> Dict[str, Any]:
        """解析 <Time><localTime/><timeZone/>...</Time> -> {"Time": {...}}。"""
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as e:
            raise RuntimeError(f"XML parse failed: {e}") from e

        def _local(tag: str) -> str:
            return tag.split("}", 1)[-1] if "}" in tag else tag

        fields: Dict[str, Any] = {}
        for el in root.iter():
            tag = _local(el.tag)
            if tag in ("localTime", "timeZone", "timeMode", "IANA"):
                fields[tag] = el.text
        return {"Time": fields}

    def set_time(self, local_time: str,
                 timezone: str | None = None) -> Dict[str, Any]:
        """PUT /ISAPI/System/time，使用 XML 报文。

        先 GET 设备当前 Time，**原样保留** ``timeMode`` 与 ``IANA`` 字段
        （海康要求 set_time 报文必须包含二者，否则返回
        ``MessageParametersLack`` / ``errorMsg=timeMode``）。仅替换
        ``localTime``（与 ``timeZone``，缺省沿用设备当前值）。
        以 application/xml 发送。返回解析后的 ResponseStatus 字典。
        """
        # 若 local_time 带微秒则去除（海康会拒绝）。
        local_time = _strip_microseconds(local_time)
        # 读取设备当前 Time，保留 timeMode / IANA，避免报文缺字段被拒。
        try:
            cur = self.get_time().get("Time", {})
        except Exception:  # noqa: BLE001
            cur = {}
        tz = timezone if timezone is not None else (cur.get("timeZone") or "CST-8:00")
        time_mode = cur.get("timeMode") or "1"
        iana = cur.get("IANA") or ""
        xml_body = (
            '<Time version="2.0" '
            'xmlns="http://www.isapi.org/ver20/XMLSchema">'
            f"<localTime>{local_time}</localTime>"
            f"<timeZone>{tz}</timeZone>"
            f"<timeMode>{time_mode}</timeMode>"
            f"<IANA>{iana}</IANA>"
            "</Time>"
        ).encode("utf-8")
        status, body = self._request(
            "PUT", "/ISAPI/System/time", body=xml_body,
            headers={"Content-Type": "application/xml"})
        if status != 200:
            raise RuntimeError(f"set_time returned {status}")
        return self._parse_status_xml(body)

    def get_work_status(self) -> Dict[str, Any]:
        """GET /ISAPI/AccessControl/AcsWorkStatus?format=json"""
        status, body = self._request(
            "GET", "/ISAPI/AccessControl/AcsWorkStatus?format=json")
        if status != 200:
            raise RuntimeError(f"get_work_status returned {status}")
        return json.loads(body.decode("utf-8"))

    def wait_online(self, timeout: float = 180.0,
                    probe_endpoint: str = "/ISAPI/System/deviceInfo",
                    probe_interval: float = 2.0) -> bool:
        """轮询 probe_endpoint 直到设备恢复在线(2 阶段:先看到失败,再看到成功)。

        用于 reboot / upgrade 等需要等待设备重启恢复的场景。阶段 1 等待首次失败
        (设备开始重启,HTTP 暂不可达);阶段 2 等待首次成功(设备 HTTP 恢复)。
        与 worker.py 的 3 阶段 _wait_online 相比,这里不要求连续 N 次成功确认,
        适合 RebootAction 的简化场景;worker.py 的 3 阶段探测将在 PR4c 删除。

        Args:
            timeout: 总超时秒数(默认 180s)
            probe_endpoint: 探测端点(默认 /ISAPI/System/deviceInfo)
            probe_interval: 轮询间隔秒数(默认 2.0s)

        Returns:
            True 设备恢复在线;False 超时未恢复
        """
        deadline = time.monotonic() + max(timeout, 1.0)
        offline_seen = False
        while time.monotonic() < deadline:
            try:
                self._request("GET", probe_endpoint)
                if offline_seen:
                    return True
            except Exception:  # noqa: BLE001
                offline_seen = True
            time.sleep(probe_interval)
        # 超时后再做一次最终探测,处理阶段 2 刚好在 deadline 边界成功的场景
        try:
            self._request("GET", probe_endpoint)
            return True
        except Exception:  # noqa: BLE001
            return False

    def query_events(self, major: int, minor: int,
                     start: str, end: str) -> List[Dict[str, Any]]:
        """POST /ISAPI/AccessControl/AcsEvent?format=json -> InfoList。"""
        payload = self._build_event_cond(major, minor, start, end)
        status, body = self._request(
            "POST", "/ISAPI/AccessControl/AcsEvent?format=json", body=payload)
        if status != 200:
            raise RuntimeError(f"query_events returned {status}")
        data = json.loads(body.decode("utf-8"))
        info_list = data.get("AcsEvent", {}).get("InfoList")
        return info_list or []

    def query_event_chain(self, open_iso: str,
                          backward_buffer: float = 30.0,
                          baseline_serials: dict[str, int] | None = None
                          ) -> dict[str, list[Dict[str, Any]]]:
        """查询 3 事件链(remote_open + lock_open + lock_close)。

        用设备时间作为终点,open_iso - backward_buffer 作为起点,分别查询 3 个事件码,
        再用 baseline_serials 过滤掉基线之前的事件。对应 worker.py 的
        _query_events_pre_reboot 逻辑(行 809-860)。

        Args:
            open_iso: 开门时刻的设备时间(ISO 8601,锚点)
            backward_buffer: 向前回溯秒数(默认 30s,覆盖 remote_open_door 触发到事件落盘的延迟)
            baseline_serials: 各事件类型的基线 serialNo,如 {"trigger": 100, "opened": 95, "closed": 90}
                缺省或某键缺失则不过滤该类型

        Returns:
            {"trigger": [...], "opened": [...], "closed": [...]}
            查询失败时返回 3 个空列表
        """
        from datetime import datetime, timedelta

        from .event_codes import HikEventCode

        # 起点 = open_iso - backward_buffer
        try:
            anchor = datetime.fromisoformat(open_iso)
        except ValueError:
            anchor = datetime.fromisoformat(open_iso.replace("Z", "+00:00"))
        start_iso = (anchor - timedelta(seconds=backward_buffer)).isoformat()

        # 终点 = 当前设备时间
        try:
            device_time = self.get_time()["Time"]["localTime"]
        except Exception:  # noqa: BLE001
            # 设备时间不可用时回退到 open_iso(已发生的事件仍可查到)
            device_time = open_iso
        end_iso = device_time

        result: dict[str, list[Dict[str, Any]]] = {
            "trigger": [], "opened": [], "closed": []}
        try:
            result["trigger"] = self.query_events(*HikEventCode.REMOTE_OPEN, start_iso, end_iso)
            result["opened"] = self.query_events(*HikEventCode.LOCK_OPEN, start_iso, end_iso)
            result["closed"] = self.query_events(*HikEventCode.LOCK_CLOSE, start_iso, end_iso)
        except Exception:  # noqa: BLE001
            return result  # 查询失败返回空列表

        # 用 baseline_serials 过滤掉基线之前的事件
        if baseline_serials:
            for name in result:
                base = baseline_serials.get(name, 0)
                result[name] = [e for e in result[name]
                                if int(e.get("serialNo", 0)) > base]
        return result

    def get_door_param(self, door_no: int = 1) -> Dict[str, Any]:
        """GET /ISAPI/AccessControl/Door/param/<door_no>。

        返回解析后的字段字典（openDuration、magneticType 等）。用正则提取
        以避免命名空间解析差异（DoorParam 带默认 xmlns）。
        """
        status, body = self._request(
            "GET", f"/ISAPI/AccessControl/Door/param/{door_no}")
        if status != 200:
            raise RuntimeError(f"get_door_param returned {status}")
        text = body.decode("utf-8", "replace")

        def _find(tag: str) -> Any:
            m = re.search(rf"<{tag}>(.*?)</{tag}>", text, re.S)
            return m.group(1).strip() if m else None

        return {"openDuration": _find("openDuration"),
                "magneticType": _find("magneticType")}

    def set_door_open_duration(self, door_no: int, seconds: int) -> Dict[str, Any]:
        """设置门锁开启保持时间 openDuration（秒，整数，设备持久化）。

        读取原始 DoorParam XML，原地替换 ``<openDuration>`` 值后整体回写，
        以保留海康原始结构（含嵌套字段），降低 PUT 因结构不全而失败的风险。

        注意：该方法为「显式、带外」配置能力，**稳定性循环不会自动调用**。
        框架默认只通过 :meth:`get_door_param` 读取当前值用于本地查询时序，
        不修改设备配置。
        """
        path = f"/ISAPI/AccessControl/Door/param/{door_no}"
        status, body = self._request("GET", path)
        if status != 200:
            raise RuntimeError(f"get_door_param returned {status}")
        text = body.decode("utf-8", "replace")
        new_text = re.sub(r"<openDuration>\d+</openDuration>",
                          f"<openDuration>{int(seconds)}</openDuration>",
                          text, count=1)
        if new_text == text:
            raise RuntimeError("DoorParam 中未找到 <openDuration> 字段")
        status, body = self._request(
            "PUT", path, body=new_text.encode("utf-8"),
            headers={"Content-Type": "application/xml"})
        if status != 200:
            raise RuntimeError(f"set_door_open_duration returned {status}")
        return self._parse_status_xml(body)

    def close(self) -> None:
        """关闭底层 httpx.Client 连接池(长生命周期客户端用完应显式释放)。"""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


__all__ = ["HikvisionClient"]
