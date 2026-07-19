"""场景化 TargetAdapter：把一份 Scenario 映射到海康 ISAPI 设备。

- ``ScenarioISAPIAdapter``：真实设备适配器，复用 ``HikvisionClient`` 的 Digest
  鉴权与通用 ``request_json``。每轮执行 stress（reboot/upgrade/issue/none），
  并在 reboot/upgrade 后等待设备重新上线。
- ``FakeScenarioAdapter``：纯内存脚本化适配器，用于 dry-run 与测试——无需真实
  设备即可验证「场景 -> 事实 -> 裁决」整条链路（满足测试不变量中的
  "使用合成适配器，不引入真实外部依赖"）。

两者都实现 ``multi_agent.adapter.TargetAdapter`` 契约（act / observe / events）。
"""

import json
import time
from typing import Any, Dict, List, Optional

from ...multi_agent.adapter import Event, Result, State, TargetAdapter
from .scenario_schema import Scenario, resolve_field


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

        分两阶段：先等一次失败（设备正在重启、HTTP 暂不可达），再等恢复成功。
        这与 HikvisionWorker._wait_online 的「离线 -> 上线」语义一致，但复用
        场景自身的 probe 端点，避免与具体字段耦合。
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


class FakeScenarioAdapter:
    """脚本化内存适配器，用于 dry-run 与测试（无真实设备依赖）。

    ``probe_values`` 是一组「每次 observe() 返回的快照」列表；第 i 次 observe
    返回 ``probe_values[min(i, len-1)]``，便于精确控制每轮的探测结果。
    ``fail_stress`` 可让 act() 返回失败（模拟重启失败 / 设备掉线）。
    """

    def __init__(self, scenario: Scenario,
                 probe_values: Optional[List[Dict[str, Any]]] = None,
                 *, fail_stress: bool = False) -> None:
        self._sc = scenario
        self._probe_values = list(probe_values or [])
        self._fail_stress = fail_stress
        self._i = 0
        self.stress_calls = 0
        self.observe_calls = 0

    def _default_probe(self) -> Dict[str, Any]:
        """缺省在线快照：按 probe.field 路径构造嵌套结构，使默认用例「通过」。

        例如 field="AcsWorkStatus.doorOnlineStatus[0]"、expect_equals=1 时，
        构造 {"AcsWorkStatus": {"doorOnlineStatus": [1]}}，正好命中断言。
        """
        import re
        p = self._sc.probe
        val = p.expect_equals if p.expect_equals is not None else (
            p.expect_in[0] if p.expect_in else "online")
        snap: Dict[str, Any] = {}
        cursor: Any = snap
        parts = list(re.finditer(r"([^.\[\]]+)(?:\[(\d+)\])?", p.field))
        for j, m in enumerate(parts):
            key = m.group(1)
            idx = m.group(2)
            is_last = j == len(parts) - 1
            if idx is not None:
                lst = cursor.setdefault(key, [])
                while len(lst) <= int(idx):
                    lst.append({} if not is_last else None)
                if is_last:
                    lst[int(idx)] = val
                else:
                    if not isinstance(lst[int(idx)], dict):
                        lst[int(idx)] = {}
                    cursor = lst[int(idx)]
            else:
                if is_last:
                    cursor[key] = val
                else:
                    cursor = cursor.setdefault(key, {})
        return snap

    def act(self, operation: Any) -> Result:
        self.stress_calls += 1
        if self._fail_stress:
            return Result(ok=False, error="fake stress failure")
        return Result(ok=True)

    def observe(self) -> State:
        self.observe_calls += 1
        if self._probe_values:
            val = self._probe_values[min(self._i, len(self._probe_values) - 1)]
        else:
            val = self._default_probe()
        self._i += 1
        return State(snapshot=val)

    def events(self, since: float) -> List[Event]:
        return []


__all__ = ["ScenarioISAPIAdapter", "FakeScenarioAdapter"]
