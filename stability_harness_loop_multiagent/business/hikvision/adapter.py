"""HikvisionAdapter: sync TargetAdapter over HikvisionClient.

Implements the TargetAdapter protocol (act/observe/events). Sync because
the protocol is sync; Worker wraps with asyncio.to_thread for parallelism.

设备连接信息（前端透传的 3 个输入框：IP、用户名、密码）经 ``device_config``
注入；``HikvisionAdapterFactory`` 是显式的工厂包装层，把
``{ip / username / password}`` 归一后产出 ``HikvisionAdapter``。
"""

from typing import Any, List

from ...multi_agent.adapter import Event, Result, State
from .client import HikvisionClient


# 前端 3 个输入框 -> 内部字段的别名归一表（大小写不敏感匹配 key 的片段）。
_DEVICE_FIELD_ALIASES = {
    "host": ("ip", "host", "address", "addr", "endpoint"),
    "username": ("username", "user", "login", "account"),
    "password": ("password", "pass", "pwd", "secret", "credential"),
    "port": ("port",),
    "http_timeout": ("http_timeout", "http_timeout_s", "timeout"),
}


def normalize_device_config(device_config) -> dict:
    """把前端/环境变量透传的设备配置归一为标准字段。

    输入可以是：
      - JSON 全量（``{"ip": ..., "username": ..., "password": ..., "port": ...}``）
      - 单项合并（host/user/pass 等别名）
      - 大小写不敏感

    归一后固定输出 ``{"host", "port", "username", "password", "http_timeout"}``；
    ``host`` 缺失则抛 ``ValueError``（设备连接的最小必要信息）。``username``
    缺省 ``admin``、``password`` 缺省空串、``port`` 缺省 80、``http_timeout``
    缺省 15.0（与 HikvisionClient 默认值对齐）。
    """
    if not isinstance(device_config, dict):
        raise ValueError(f"device_config 需为 dict，收到：{type(device_config).__name__}")
    norm: dict = {}
    # 先把所有 key 小写化后建立查找表，便于别名匹配。
    lowered = {str(k).lower(): v for k, v in device_config.items()}
    for std, aliases in _DEVICE_FIELD_ALIASES.items():
        for alias in aliases:
            if alias in lowered:
                norm[std] = lowered[alias]
                break
    host = norm.get("host")
    if not host:
        raise ValueError(
            "device_config 缺少设备地址（需 'ip' / 'host' / 'address' 之一）"
        )
    port = norm.get("port")
    try:
        port = int(port) if port is not None else 80
    except (TypeError, ValueError):
        port = 80
    http_timeout = norm.get("http_timeout")
    try:
        http_timeout = float(http_timeout) if http_timeout is not None else 15.0
    except (TypeError, ValueError):
        http_timeout = 15.0
    return {
        "host": str(host),
        "port": port,
        "username": str(norm.get("username", "admin")),
        "password": str(norm.get("password", "")),
        "http_timeout": http_timeout,
    }


class HikvisionAdapter:
    """Sync TargetAdapter implementation for Hikvision door access.

    构造方式（二选一，向后兼容）：
      - ``HikvisionAdapter(client)``                  —— 直接传入已构造的 client。
      - ``HikvisionAdapter(device_config={...})``     —— 由 ``device_config``
        （IP/用户名/密码）自动构造 HikvisionClient（前端经 os.env 透传的推荐路径）。
    """

    def __init__(self, client=None, *, device_config=None) -> None:
        if device_config is not None:
            cfg = normalize_device_config(device_config)
            client = HikvisionClient(
                host=cfg["host"], port=cfg["port"],
                username=cfg["username"], password=cfg["password"],
                http_timeout=cfg["http_timeout"],
            )
        if client is None:
            raise ValueError(
                "HikvisionAdapter 需提供 client= 或 device_config= 之一"
            )
        self._client = client

    def act(self, operation: Any) -> Result:
        """Execute an operation dict {op: ..., ...}."""
        if not isinstance(operation, dict):
            return Result(ok=False, error="operation must be dict")
        op = operation.get("op")
        try:
            if op == "remote_open_door":
                data = self._client.remote_open_door(operation.get("door_no", 1))
                return Result(ok=True, data=data)
            if op == "reboot":
                data = self._client.reboot()
                return Result(ok=True, data=data)
            if op == "set_time":
                data = self._client.set_time(
                    operation["local_time"], operation.get("timezone", "CST-8:00")
                )
                return Result(ok=True, data=data)
            return Result(ok=False, error=f"unknown op: {op}")
        except Exception as exc:  # noqa: BLE001
            return Result(ok=False, error=str(exc))

    def observe(self) -> State:
        """Probe device work status."""
        try:
            snap = self._client.get_work_status()
            return State(snapshot=snap)
        except Exception as exc:  # noqa: BLE001
            return State(snapshot={"error": str(exc)})

    def events(self, since: float) -> List[Event]:
        """Return events since epoch seconds (best-effort mapping).

        Adapter protocol uses epoch; business layer typically queries by
        ISO window via client.query_events directly in Worker. Here we
        return an empty list as the generic surface is not used by Worker.
        """
        return []


class HikvisionAdapterFactory:
    """海康适配器工厂（前端包装层）。

    把前端经 os.env 透传的「设备连接信息」——典型就是 3 个输入框
    （IP、用户名、密码）——归一为 ``device_config``，再产出可直接注入
    ``run_generic(target_adapter=...)`` 的 ``HikvisionAdapter``。

    用法（前端代码）：
        adapter = HikvisionAdapterFactory.create({
            "ip": "10.0.0.1", "username": "admin", "password": "***",
        })
        # 或等价地：HikvisionAdapterFactory.create(ip=..., username=..., password=...)

    等价地也可经通用入口 ``STABILITY_REAL_TARGET=...:HikvisionAdapter`` +
    ``STABILITY_DEVICE_IP/USERNAME/PASSWORD`` 透传（见 examples/generic_harness）。
    """

    @staticmethod
    def create(device_config: dict | None = None, *, client=None,
               ip: str | None = None, username: str | None = None,
               password: str | None = None, port: int | None = None,
               http_timeout: float | None = None) -> "HikvisionAdapter":
        """产出 HikvisionAdapter。

        设备连接信息两种来源（可二选一）：
          - ``device_config`` dict（含 ip/username/password 等，别名均可）。
          - 直接传 ``ip``/``username``/``password`` 等关键字（3 输入框直传）。
        若 ``client`` 已存在则直接包装（跳过构造）。
        """
        if client is not None:
            return HikvisionAdapter(client)
        if device_config is None:
            device_config = {}
        # 关键字参数合并进 device_config（关键字优先），方便前端 3 输入框直传。
        merged = dict(device_config)
        for k, v in (("ip", ip), ("username", username),
                     ("password", password), ("port", port),
                     ("http_timeout", http_timeout)):
            if v is not None:
                merged[k] = v
        cfg = normalize_device_config(merged)
        client = HikvisionClient(
            host=cfg["host"], port=cfg["port"],
            username=cfg["username"], password=cfg["password"],
            http_timeout=cfg["http_timeout"],
        )
        return HikvisionAdapter(client)


__all__ = ["HikvisionAdapter", "HikvisionAdapterFactory", "normalize_device_config"]
