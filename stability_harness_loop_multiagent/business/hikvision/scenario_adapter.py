"""场景化 TargetAdapter：把一份 Scenario 映射到海康 ISAPI 设备。

``ScenarioISAPIAdapter`` 是真实设备适配器，复用 ``HikvisionClient`` 的 Digest
鉴权与通用 ``request_json``。每轮执行 stress（reboot/upgrade/issue/none），
并在 reboot/upgrade 后等待设备重新上线。实现 ``multi_agent.adapter.TargetAdapter``
契约（act / observe / events）。
"""

import json
import time
from typing import Any, List

from ...multi_agent.adapter import Event, Result, State, TargetAdapter
from .scenario_schema import Scenario


class ScenarioISAPIAdapter:
    """真实海康 ISAPI 设备的场景适配器。"""

    def __init__(self, client, scenario: Scenario) -> None:
        self._client = client
        self._sc = scenario

    # ---- TargetAdapter 契约 -------------------------------------------
    def act(self, operation: Any) -> Result:
        st = self._sc.stress
        if st.type == "none":
            return Result(ok=True)
        if st.type == "reboot":
            try:
                self._client.reboot()
            except Exception as exc:  # noqa: BLE001
                return Result(ok=False, error=f"reboot 失败: {exc}")
            online = self._wait_online(st.wait_online_timeout)
            return Result(ok=online, error=None if online else "重启后未上线")
        # upgrade / issue：按配置发请求；upgrade 默认也会重启设备。
        try:
            body = self._resolve_body(st)
            self._client.request_json(st.method or "PUT", st.endpoint, body)
        except Exception as exc:  # noqa: BLE001
            return Result(ok=False, error=f"stress({st.type}) 失败: {exc}")
        if st.reboot_after:
            online = self._wait_online(st.wait_online_timeout)
            return Result(ok=online, error=None if online else "操作后设备未上线")
        return Result(ok=True)

    def observe(self) -> State:
        """GET 探测端点并解析为 JSON 快照。"""
        p = self._sc.probe
        data = self._client.request_json(p.method or "GET", p.endpoint)
        return State(snapshot=data)

    def events(self, since: float) -> List[Event]:
        return []

    # ---- 内部辅助 ------------------------------------------------------
    @staticmethod
    def _resolve_body(st: Any) -> Any:
        if st.body_file:
            with open(st.body_file, "r", encoding="utf-8") as f:
                text = f.read()
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text
        return st.body

    def _wait_online(self, timeout: float) -> bool:
        """轮询探测端点是否可访问（能 GET 到即视为设备已上线）。

        分两阶段：先等一次失败（设备正在重启、HTTP 暂不可达），再等恢复成功，
        复用场景自身的 probe 端点，避免与具体字段耦合。
        """
        deadline = time.monotonic() + max(timeout, 1.0)
        offline_seen = False
        probe_ep = self._sc.probe.endpoint
        while time.monotonic() < deadline:
            try:
                self._client.request_json("GET", probe_ep)
                if offline_seen:
                    return True
            except Exception:  # noqa: BLE001
                offline_seen = True
            time.sleep(2.0)
        try:
            self._client.request_json("GET", probe_ep)
            return True
        except Exception:  # noqa: BLE001
            return False


__all__ = ["ScenarioISAPIAdapter"]
