"""HikvisionWorker：开门测试执行 + 事件链断言 + 自愈。

流水线对齐人工检查逻辑：
  pre_loop_setup()  -> 记录初始基线 + 基准重启 + 记录 baseline_reboot_duration
                       （离线 -> 恢复 -> 确认）。在 Loop 启动前调用一次。
  do_work(tick)     -> remote_open_door -> sleep(event_check_delay) ->
                       查询事件（设备在线时）-> 重启 ->
                       wait(baseline_reboot_duration) -> 验证在线。
                       （仅当 run_reboot=True 且 plan.skip_reboot=False 时
                       才执行重启阶段）
  recover(tick)     -> 异步：事件缺失时可选自愈；用本轮事件更新基线 serialNo。
  check(tick)       -> 同步：基于缓存结果断言 3 事件链事实。

spec §3.1.3 长 IO 规则：do_work 可能阻塞 60-180s（重启 + 探测 + 预热），
因此 act() 用 asyncio.to_thread 包裹，避免阻塞事件循环。
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ...core.agent import AgentSpec
from ...core.bus import EventBus
from ...multi_agent.workers.base import WorkerAgent
from .adapter import HikvisionAdapter
from .diagnostic import DiagnosticKernel, HEAL_TIME_SYNC, HEAL_ABORT
from .event_codes import HikEventCode


def _now_iso() -> str:
    t = datetime.now(timezone(timedelta(hours=8)))
    return t.strftime("%Y-%m-%dT%H:%M:%S+08:00")


def _iso_seconds_before(ref_iso: str, seconds: float) -> str:
    """在某个参考 ISO 时间之前 N 秒的 ISO 时间戳（秒级精度）。

    用于计算基于设备时间的查询窗口（spec §2.1 A.2）：设备事件日志以设备时间
    记录事件，因此窗口必须使用设备时间。重启后设备时间可能发生漂移
    （NTP 未同步、出厂默认），若用主机时间窗口会漏掉漂移时间下记录的事件。
    seconds 可为小数。
    """
    ref = datetime.fromisoformat(ref_iso)
    t = ref - timedelta(seconds=seconds)
    return t.strftime("%Y-%m-%dT%H:%M:%S+08:00")


def _iso_add_seconds(ref_iso: str, seconds: float) -> str:
    """在某个参考 ISO 时间之后 N 秒的 ISO 时间戳（秒级精度）。

    与 ``_iso_seconds_before`` 对称，用于以设备时间为锚推算后续阶段时间戳
    （本机时钟与设备时钟存在偏差时，保证时间线全程使用设备时钟）。
    seconds 可为小数（可为负，等价于前移）。
    """
    ref = datetime.fromisoformat(ref_iso)
    t = ref + timedelta(seconds=seconds)
    return t.strftime("%Y-%m-%dT%H:%M:%S+08:00")


# 允许通过 --set 显式写入设备的参数白名单：键 -> 说明。
# 只有列在这里的键才会被 PUT；其余键一律忽略（默认 GET，不修改设备）。
# 新增可写能力只需在此登记，无需新增 CLI 指令（避免“每功能一个指令”）。
_DEVICE_WRITE_WHITELIST: Dict[str, str] = {
    "openDuration": "门锁开启保持时间（秒）",
}


class HikvisionWorker(WorkerAgent):
    def __init__(self, bus: EventBus, spec: AgentSpec, adapter: HikvisionAdapter,
                 client, time_skew_threshold: float = 3.0,
                 diagnostic: DiagnosticKernel = None,
                 *,
                 run_reboot: bool = True,
                 probe_interval: float = 5.0,
                 probe_confirm_count: int = 2,
                 warmup_time: float = 60.0,
                 max_recover_timeout: float = 180.0,
                 event_check_delay: float = 3.0,
                 open_duration: float | None = None,
                 device_writes: Dict[str, Any] | None = None,
                 required_serial_mode: str | None = None,
                 serial_port: int = 1,
                 governance: "Governance | None" = None,
                 enable_governance: bool = False,
                 governance_timeout: float = 1.0) -> None:
        super().__init__(bus, spec, adapter)
        self._client = client
        self._time_skew_threshold = time_skew_threshold
        self._diagnostic = diagnostic
        self._last_events: Dict[str, list] = {"trigger": [], "opened": [], "closed": []}
        # 前置条件：期望的串口外设类型（mode）。若为 None，则不自动切换串口，
        # 仅在门离线时判定前置条件失败（需人工介入）。若为某具体 mode（如
        # "externMode"），则当前 mode 不符时自动切换并等待设备自动重启上线。
        self._required_serial_mode = required_serial_mode
        self._serial_port = serial_port
        # 治理闸门（opt-in）：默认关闭。开启后，act() 在 to_thread(do_work) 之前
        # 发一次 harness/govern/request（operation="round"）做每轮粗粒度护栏。
        # fail-closed：超时/无治理代理时一律视为拒绝。纯总线契约，不 import 治理实现。
        self._enable_governance = enable_governance
        self._governance_timeout = governance_timeout
        # 熔断器护栏（opt-in）：仅当传入 governance 且其中含名为 "hikvision-api"
        # 的 CircuitBreaker 时生效。破坏性外部操作（开门/重启）经 _guarded_adapter_act
        # 检查熔断器状态，打开则跳过并上报，避免对设备疯狂重试。business/ 属于领域
        # 代码，可引用 harness.governance（三引擎约束只限制 harness/loop/multi_agent
        # 互不直接 import）；此处仅运行时调用其方法，类型标注用字符串避免顶层 import。
        self._governance = governance
        # 事件链跨轮统计：累积每轮的 3 事件链计数与离线成因计数。
        self._chain_stats: Dict[str, int] = {
            "rounds": 0,
            "trigger": 0, "opened": 0, "closed": 0,
            "in_loop_offline": 0, "precond_fixed": 0, "precond_failed": 0,
        }
        # 最近一次门离线的成因：None / "in_loop_device_problem"（循环中设备故障）
        # / "precond"（前置条件未就绪）。用于区分「循环内设备故障」与
        # 「前置条件未准备」。
        self._last_offline_cause: str | None = None
        self._precond: Dict[str, Any] = {}
        self._healed: Any = None
        # 时间线设备时间锚：setup 时读取一次设备时间，后续阶段时间戳按
        # monotonic 偏移推算，使整条时间线使用设备时钟（与本机时钟偏差无关），
        # 便于与设备事件日志的设备时间对齐。基准缺失时回退本机时间。
        self._dev_ref_iso: str = ""
        self._dev_ref_mono: float = 0.0
        # 重启 + 探测 + 预热配置（spec §4.1、§4.2、§6 worker.*）
        self._run_reboot = run_reboot
        self._probe_interval = probe_interval
        self._probe_confirm_count = probe_confirm_count
        self._warmup_time = warmup_time
        self._max_recover_timeout = max_recover_timeout
        # remote_open_door PUT 与事件日志查询之间的延迟（spec §6
        # worker.event_check_delay）。真实设备会在协议完成后 2-3s 才将事件
        # 写入日志；查询过早会漏掉事件。
        self._event_check_delay = event_check_delay
        # 门锁开启保持时间（openDuration，单位秒）等可写设备参数。
        # 边界原则（用户指示）：默认只 GET，不主动修改设备配置。
        #   - open_duration：本地查询时序覆盖值（测试 / 离线用），不写入设备。
        #   - device_writes：用户**显式指示**要写入设备的参数（PUT），来自
        #     统一的 --set KEY=VALUE 通道；仅有白名单内的键才会被写入。
        #     未列出的参数一律从设备读取（GET）。这样新增可写能力无需新增指令。
        # 生效值与来源（device / device(written) / override / fallback）在
        # pre_loop_setup() 中确定。
        self._open_duration_override = open_duration
        self._device_writes = device_writes or {}
        self._open_duration: float = 2.0
        self._open_duration_source: str = "device"
        # 记录最近一次 do_work 的结果，便于观测
        self._last_work_stages: Dict[str, Any] = {}
        # 每轮各阶段执行的时间线，便于观测。
        # 每条记录：{"stage": <阶段名>, "ts": <iso>, "t": <自本轮起点的秒数>, **extra}
        # 在每次 do_work() 开始时重置；recover()/check() 也会追加记录。
        self._timeline: List[Dict[str, Any]] = []
        # 跨轮累积的完整时间线，供 SUMMARY 统计（总测试时长需「跨轮真实墙钟」）。
        # 与 _timeline（每轮重置、供当轮打印）分离，避免相互干扰。
        self._full_timeline: List[Dict[str, Any]] = []
        self._t0: float = 0.0  # 当前轮的单调计时起点
        # 在 pre_loop_setup() 中记录的基线：保存各事件类型（remote_open、
        # lock_open、lock_close）最新的 serialNo，使得重启后的查询能过滤掉
        # 已有事件，只统计本轮 remote_open_door 产生的新事件。
        # 这解决了“trigger=2”问题——即回看窗口里混入了上一轮的残余事件。
        self._baseline: Dict[str, Any] = {}
        self._baseline_recorded: bool = False
        # pre_loop_setup() 基准重启测量得到的重启耗时。
        # 后续每轮重启会等待这段时长（而非每次都跑完整的 3 阶段探测），
        # 因为设备重启耗时大致恒定。若未执行 setup，则回退到每轮运行
        # _wait_online() 探测。
        self._baseline_reboot_duration: float = 0.0
        self._setup_done: bool = False
        # do_work() 的缓存事实：事件在每轮重启前、设备在线时查询；recover()
        # 在重启之后运行，因此无法自行查询事件。check() 读取这些缓存事实。
        self._round_online_ok: bool = False

    async def act(self, tick: dict) -> None:
        """覆写基类 act()，在线程中运行 do_work。

        do_work 可能阻塞 60-180s（重启 + 探测 + 预热）。按 spec §3.1.3，
        长 IO 必须用 asyncio.to_thread 包裹，以避免阻塞事件循环、
        导致 ControlLoop 的超时安全网失效。

        每轮粗粒度治理闸门：仅在 enable_governance 时、且 to_thread 之前执行
        （异步入口，因 do_work 是同步的，无法在其内部 await 总线）。拒绝则跳过
        本轮破坏性操作并上报 denied 事实；若仅部分操作被拒（denied_ops），则把
        被拒操作集合传入 do_work 由其跳过对应步骤。
        """
        denied_ops: set = set()
        if self._enable_governance:
            decision = await self._governance_decision(tick)
            if decision is None or not decision.get("allowed", False):
                self._publish_governance_denied(tick)
                return
            denied_ops = set(decision.get("denied_ops") or [])
            if "remote_open_door" in denied_ops:
                # 核心测试操作被拒 -> 整轮跳过（与整轮拒绝同处理）
                self._publish_governance_denied(tick)
                return
        result = await asyncio.to_thread(self.do_work, tick, denied_ops)
        self.publish(
            "target/acted",
            {"role": self.role, "round": tick.get("round"), "result": result},
        )
        recovered = await self.recover(tick)
        self.publish(
            "target/recovered",
            {"role": self.role, "round": tick.get("round"), "recovered": recovered},
        )
        facts = self.check(tick)
        self.publish(
            "target/checked",
            {"role": self.role, "round": tick.get("round"), "facts": facts},
        )
        self.publish("agent/" + self.role + "/done", {"round": tick.get("round")})

    async def _governance_decision(self, tick: dict) -> Optional[Dict[str, Any]]:
        """每轮粗粒度治理闸门（fail-closed，纯总线契约），返回富回复。

        向 ``harness/govern/request`` 发起一次 request/reply，返回完整回复字典
        （含 ``allowed`` 与 ``denied_ops``）；超时/异常一律返回 ``None``（视为
        整轮拒绝）。请求的 ``operations`` 携带本轮计划操作，便于治理按操作鉴权。

        仅用 ``self.request``（core Agent 能力）与话题字符串，不 import 治理实现，
        保持与三引擎约束一致。
        """
        plan = self.state.get("plan", {}) or {}
        skip_reboot = bool(plan.get("skip_reboot", False))
        ops = ["remote_open_door"]
        if self._run_reboot and not skip_reboot:
            ops.append("reboot")
        req = {
            "role": self.role,
            "capability": "door-test",
            "operation": "round",
            "round": tick.get("round"),
            "operations": ops,
        }
        try:
            reply = await self.request(
                "harness/govern/request", req, timeout=self._governance_timeout
            )
        except Exception:  # noqa: BLE001 - 总线超时/错误一律视为拒绝
            # 调用方 fail-closed 超时路径：网关从未回复，故在此发一条结构化事实，
            # 让观测面板看到「哪一轮因治理不可达被拒」。与网关决策事实不重复。
            tel = self._governance.telemetry if self._governance else None
            if tel is not None:
                tel.fact(
                    "governance.decision", allowed=False,
                    reason="timeout/fail-closed", denied_ops=[],
                    role=req.get("role"), capability=req.get("capability"),
                    operation=req.get("operation"), round=req.get("round"),
                )
            return None
        if not isinstance(reply, dict):
            return None
        return reply

    def _guarded_adapter_act(self, op: dict) -> Any:
        """破坏性外部操作的熔断器护栏（opt-in）。

        仅当 ``governance`` 含名为 ``"hikvision-api"`` 的 CircuitBreaker 时生效：
        - 熔断器打开则跳过该操作，返回失败 Result（不再对设备疯狂重试）。
        - 操作成功/失败均记录到熔断器，使其在连续失败达到阈值时自动打开。
        无熔断器配置时退化为直接调用 ``adapter.act``。
        """
        if self._governance is not None and not self._governance.breaker_allow("hikvision-api"):
            self._mark("breaker_open", op=op.get("op"))
            from ...multi_agent.adapter import Result
            return Result(ok=False, error="circuit breaker open")
        res = self.adapter.act(op)
        if self._governance is not None:
            self._governance.breaker_record("hikvision-api", res.ok)
        return res

    def _publish_governance_denied(self, tick: dict) -> None:
        """治理拒绝时跳过 do_work，并发布 denied 事实，使循环不变量仍满足。"""
        self._mark("governance_denied", role=self.role, round=tick.get("round"))
        self.publish(
            "target/acted",
            {"role": self.role, "round": tick.get("round"),
             "result": {"denied": True, "reason": "governance"}},
        )
        self.publish(
            "target/recovered",
            {"role": self.role, "round": tick.get("round"), "recovered": False},
        )
        self.publish(
            "target/checked",
            {"role": self.role, "round": tick.get("round"),
             "facts": {"governance_denied": True, "recovered": False}},
        )
        self.publish("agent/" + self.role + "/done", {"round": tick.get("round")})

    def _mark(self, stage: str, **extra: Any) -> None:
        """向每轮时间线追加一条阶段记录。

        线程安全：list.append 在 CPython GIL 下是原子操作，因此来自 do_work
        （运行于工作线程）与 recover/check（运行于事件循环）的调用都能
        安全追加。阶段时间戳使用设备时间（见 ``_device_anchored_iso``），
        保证整条时间线与设备事件日志在同一时钟下，便于排查时序。
        """
        entry = {"stage": stage, "ts": self._device_anchored_iso(),
                 "t": round(time.monotonic() - self._t0, 2)}
        entry.update(extra)
        self._timeline.append(entry)
        self._full_timeline.append(entry)  # 跨轮累积，供 SUMMARY 统计总时长

    def _device_anchored_iso(self) -> str:
        """以设备时间为锚的阶段时间戳。

        以 setup 时读取的设备时间为基准，按 monotonic 偏移增量推算，避免每阶段
        都打一次网络请求。基准缺失（如 setup 时设备不可达）时回退本机时间。
        """
        if self._dev_ref_iso:
            return _iso_add_seconds(self._dev_ref_iso,
                                    time.monotonic() - self._dev_ref_mono)
        return _now_iso()

    def _resolve_open_duration(self) -> None:
        """确定门锁开启保持时间 openDuration 的生效值（只读，不写入设备）。

        规则：
          - 若构造时给定 open_duration（测试 / 离线覆盖），直接使用，不读取设备；
            来源标记为 ``override``。
          - 否则从设备 GET /ISAPI/AccessControl/Door/param/1 读取当前值；
            来源标记为 ``device``。
          - 读取失败则保守回退到 2.0s（海康常见默认），来源标记为 ``fallback``。

        注意：本方法不会修改设备配置；openDuration 仅用于本地查询时序计算。
        """
        door_no = 1
        # 白名单过滤用户显式写入意图：仅允许列表内的键 PUT，其余忽略并告警。
        unknown = [k for k in self._device_writes if k not in _DEVICE_WRITE_WHITELIST]
        if unknown:
            self._mark("device_write_unsupported", keys=unknown)
        # 显式指示：将 openDuration 写入设备（PUT）。这是用户通过 --set 明确
        # 要求的修改，不属于「默认只 GET」的边界内；写入后读回确认生效值。
        if "openDuration" in self._device_writes:
            target = float(self._device_writes["openDuration"])
            try:
                self._client.set_door_open_duration(door_no, int(round(target)))
                dev = self._client.get_door_param(door_no)
                self._open_duration = float(dev.get("openDuration") or target)
                self._open_duration_source = "device(written)"
                self._mark("open_duration_written", requested=target,
                           value=self._open_duration)
            except Exception as exc:  # noqa: BLE001
                self._open_duration = target
                self._open_duration_source = "fallback"
                self._mark("open_duration_write_failed",
                           reason=f"写入 DoorParam 失败: {exc}",
                           used=self._open_duration)
            return
        if self._open_duration_override is not None:
            self._open_duration = float(self._open_duration_override)
            self._open_duration_source = "override"
            self._mark("open_duration_override", value=self._open_duration)
            return
        try:
            dev = self._client.get_door_param(door_no)
            dev_val = float(dev.get("openDuration") or 2.0)
            self._open_duration = dev_val
            self._open_duration_source = "device"
            self._mark("open_duration_read", value=dev_val)
        except Exception as exc:  # noqa: BLE001
            self._open_duration = 2.0
            self._open_duration_source = "fallback"
            self._mark("open_duration_read_failed",
                       reason=f"读取 DoorParam 失败: {exc}", used=self._open_duration)

    def _record_baseline(self) -> None:
        """记录设备基线：设备时间 + 各事件最新的 serialNo。

        记录各事件类型（remote_open、lock_open、lock_close）最新的 serialNo，
        以便后续查询能过滤掉已有事件。在 pre_loop_setup() 中、基准重启前调用，
        且每轮之后会更新 serialNo。

        若不做这一步，回看窗口会混入上一轮的残余事件，导致计数虚高
        （例如 trigger=2 而非 1）。
        """
        try:
            device_time = self._client.get_time()["Time"]["localTime"]
        except Exception:  # noqa: BLE001
            device_time = _now_iso()
        # 查询近期事件（回看 5 分钟）以找到各事件最新的 serialNo
        start = _iso_seconds_before(device_time, 300)
        serials: Dict[str, int] = {}
        for name, code in (("trigger", HikEventCode.REMOTE_OPEN),
                           ("opened", HikEventCode.LOCK_OPEN),
                           ("closed", HikEventCode.LOCK_CLOSE)):
            try:
                events = self._client.query_events(*code, start, device_time)
                serials[name] = max(
                    (int(e.get("serialNo", 0)) for e in events),
                    default=0)
            except Exception:  # noqa: BLE001
                serials[name] = 0
        self._baseline = {"device_time": device_time, "serialNos": serials}
        self._baseline_recorded = True

    def _update_baseline_from_events(self) -> None:
        """用本轮已恢复的事件更新基线 serialNo。

        在 recover() 末尾调用，使下一轮的过滤以最新已知的 serialNo 为基线。
        这保证每轮只统计上一轮之后产生的事件。
        """
        if not self._baseline_recorded:
            return
        serials = self._baseline.setdefault("serialNos", {})
        for name in ("trigger", "opened", "closed"):
            events = self._last_events.get(name, [])
            if events:
                current_max = max(int(e.get("serialNo", 0)) for e in events)
                if current_max > serials.get(name, 0):
                    serials[name] = current_max

    def _is_door_online(self, port: int = 1) -> bool:
        """读取门禁工作状态中的 doorOnlineStatus，判断门是否在线。

        海康 ``AcsWorkStatus.doorOnlineStatus`` 为列表，元素 ``1`` 表示在线，
        ``2`` 表示离线（真实设备在门离线时返回 ``[2]``）。读不到或值为非 1
        一律视为离线。
        """
        try:
            st = self._client.get_work_status().get("AcsWorkStatus", {})
            online = st.get("doorOnlineStatus") or []
            return int(online[0]) == 1 if online else False
        except Exception:  # noqa: BLE001
            return False

    def _wait_door_online(self, timeout: float = 10.0,
                         interval: float = 1.0) -> bool:
        """轮询门在线状态，容忍短暂离线（设备状态抖动）。

        用于前置条件检查：连接即时门可能处于瞬态离线，轮询一段时间若恢复在线
        即视为前置条件就绪，避免把「瞬间抖动」误判为「前置条件未准备」。已在线
        则立即返回 True。
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._is_door_online(self._serial_port):
                return True
            time.sleep(interval)
        return self._is_door_online(self._serial_port)

    def _ensure_preconditions(self) -> Dict[str, Any]:
        """Loop 前的前置条件就绪检查（用户场景：门离线可能是串口外设类型不对）。

        门离线有两种成因，必须区分（对应事实独裁与自愈的边界）：

        - **循环前置条件未准备好**：串口 1 的外设类型（``mode``）非预期值，
          导致门 1 离线。此时应切换串口外设类型（PUT），设备会自动重启
          （``statusCode=7`` / ``autoReboot``），等待上线即视为前置条件准备成功。
        - **循环过程中离线**：设备真实故障 → 由 ``check()`` 事实独裁判 ``fail``
          （见 ``act_door_offline`` 标记）。

        仅当构造时给定 ``required_serial_mode`` 才启用串口模式自愈；否则只做门
        在线校验，离线即判定前置条件失败（需人工介入，不静默通过）。
        """
        self._mark("precond_start", required_mode=self._required_serial_mode)
        info: Dict[str, Any] = {"satisfied": False, "serial_fixed": False,
                                "cause": None}

        # 分支 A：未配置串口模式要求 → 仅校验门在线（容忍短暂抖动）。
        if self._required_serial_mode is None:
            online = self._wait_door_online()
            info["door_online"] = online
            if online:
                self._mark("precond_door_ok")
                info["satisfied"] = True
            else:
                self._mark("precond_door_offline",
                           note="门离线且未配置串口模式切换，无法自愈")
                info["cause"] = "precond"
                self._chain_stats["precond_failed"] += 1
            self._mark("precond_done", **info)
            return info

        # 分支 B：配置了 required_serial_mode → 校验能力、按需切换并等待重启。
        # 1) 读取 capabilities，确认 required mode 受支持。
        try:
            caps = self._client.get_serial_capabilities(self._serial_port)
        except Exception as exc:  # noqa: BLE001
            self._mark("precond_capabilities_failed", reason=str(exc))
            info["cause"] = "precond"
            self._mark("precond_done", **info)
            return info
        supported_modes = caps.get("mode") or []
        if self._required_serial_mode not in supported_modes:
            self._mark("precond_mode_unsupported",
                       required=self._required_serial_mode,
                       supported=supported_modes)
            info["cause"] = "precond"
            info["satisfied"] = False
            self._mark("precond_done", **info)
            return info

        # 2) 读取当前配置，确认是否已是预期 mode。
        try:
            cfg = self._client.get_serial_config(self._serial_port)
        except Exception as exc:  # noqa: BLE001
            self._mark("precond_config_failed", reason=str(exc))
            info["cause"] = "precond"
            self._mark("precond_done", **info)
            return info
        current_mode = cfg.get("mode")
        info["current_mode"] = current_mode
        info["supported_modes"] = supported_modes

        if current_mode == self._required_serial_mode:
            self._mark("precond_serial_ok", mode=current_mode)
        else:
            self._mark("precond_serial_mismatch",
                       current=current_mode, required=self._required_serial_mode)
            # 切换串口外设类型：回写完整配置，仅替换 mode。
            new_fields = dict(cfg)
            new_fields["mode"] = self._required_serial_mode
            try:
                res = self._client.set_serial_config(self._serial_port, new_fields)
            except Exception as exc:  # noqa: BLE001
                self._mark("precond_serial_put_failed", reason=str(exc))
                info["cause"] = "precond"
                self._mark("precond_done", **info)
                return info
            # 设备可能要求自动重启（statusCode=7 / subStatusCode=autoReboot）。
            sub = res.get("subStatusCode")
            sc = res.get("statusCode")
            if sub == "autoReboot" or sc == 7:
                self._mark("precond_serial_reboot_required", response=res)
                # 等待设备重启并上线（复用 3 阶段探测：离线→上线→连续确认）。
                online = self._wait_online()
                self._mark("precond_serial_reboot_wait", online=online)
                if not online:
                    self._mark("precond_serial_reboot_failed")
                    info["cause"] = "precond"
                    self._mark("precond_done", **info)
                    return info
            # 重启后重新读取配置，确认 mode 已变更。
            try:
                new_cfg = self._client.get_serial_config(self._serial_port)
                info["new_mode"] = new_cfg.get("mode")
                if new_cfg.get("mode") == self._required_serial_mode:
                    self._mark("precond_serial_fixed",
                               mode=self._required_serial_mode)
                    info["serial_fixed"] = True
                    self._chain_stats["precond_fixed"] += 1
                else:
                    self._mark("precond_serial_mismatch_after",
                               current=new_cfg.get("mode"),
                               required=self._required_serial_mode)
                    info["cause"] = "precond"
                    self._mark("precond_done", **info)
                    return info
            except Exception as exc:  # noqa: BLE001
                self._mark("precond_serial_confirm_failed", reason=str(exc))
                info["cause"] = "precond"
                self._mark("precond_done", **info)
                return info

        # 3) 最终校验门在线（串口模式就绪后门应上线，容忍短暂抖动）。
        online = self._wait_door_online()
        info["door_online"] = online
        if online:
            self._mark("precond_door_ok")
            info["satisfied"] = True
        else:
            self._mark("precond_door_offline",
                       note="串口模式已就绪但门仍离线（疑似设备真实故障）")
            info["cause"] = "precond"
            self._chain_stats["precond_failed"] += 1
        self._mark("precond_done", **info)
        return info

    def pre_loop_setup(self) -> Dict[str, Any]:
        """Loop 启动前的一次性准备（人工检查步骤 1-3）。

        对应人工在 Loop 启动前的检查：
          1. 检查并记录设备初始状态（基线 serialNo）。
          2. 触发一次基准重启。
          3. 测量重启耗时（离线 -> 恢复 -> 确认）。
          4. 标记准备完成；后续每轮等待该时长，而非运行完整的 3 阶段探测。

        返回含基线与耗时的字典，便于观测。若某步骤失败，耗时保留为 0.0，
        则每轮重启回退到运行完整的 _wait_online() 探测。
        """
        self._timeline = []
        self._full_timeline = []  # 每次 run 从基准重启阶段起重新累积
        self._t0 = time.monotonic()
        # 建立设备时间锚：setup 开头读取一次设备时间，后续阶段时间戳按 monotonic
        # 偏移推算，使整条时间线（含前置条件、每轮阶段）使用设备时钟，与设备
        # 事件日志对齐。读取失败（设备暂不可达）则回退本机时间，不阻断流程。
        try:
            self._dev_ref_iso = self._client.get_time()["Time"]["localTime"]
            self._dev_ref_mono = time.monotonic()
        except Exception:  # noqa: BLE001
            self._dev_ref_iso = ""
        self._mark("setup_start")
        # 步骤 0：前置条件就绪检查（串口外设类型 / 门在线）。门离线有两种成因，
        # 必须区分：循环中离线=设备真实故障（由事实独裁判 fail）；前置条件未
        # 准备=串口外设类型(mode)不对，可切换并等待设备自动重启上线自愈。
        precond = self._ensure_preconditions()
        # 步骤 1：记录初始基线（设备时间 + serialNo）。
        self._mark("setup_baseline_start")
        self._record_baseline()
        self._mark("setup_baseline_done", **self._baseline)

        # 步骤 1.5：确定门锁开启保持时间 openDuration 的生效值（只读，不写入
        # 设备）。从设备 GET 当前值（或取构造覆盖值），供每轮查询时序使用。
        self._resolve_open_duration()

        duration = 0.0
        if self._run_reboot:
            # 步骤 2：触发基准重启。
            self._mark("setup_reboot_start")
            reboot_res = self._guarded_adapter_act({"op": "reboot"})
            self._mark("setup_reboot_done", ok=reboot_res.ok,
                       error=None if reboot_res.ok else reboot_res.error)
            if reboot_res.ok:
                # 步骤 3：通过 3 阶段探测测量重启耗时。
                self._mark("setup_probe_start",
                           interval=self._probe_interval,
                           confirm=self._probe_confirm_count)
                t_start = time.monotonic()
                online = self._wait_online()
                duration = time.monotonic() - t_start
                self._mark("setup_probe_done",
                           online=online, duration=round(duration, 2))
                if online:
                    # 可选预热，使后续每轮从完全稳定的设备状态开始
                    # （事件日志已刷新等）。
                    self._mark("setup_warmup_start", seconds=self._warmup_time)
                    time.sleep(self._warmup_time)
                    self._mark("setup_warmup_done")
        self._baseline_reboot_duration = duration
        self._setup_done = True
        self._mark("setup_done",
                   baseline_reboot_duration=round(duration, 2))
        return {"baseline": dict(self._baseline),
                "baseline_reboot_duration": duration,
                "open_duration": self._open_duration,
                "open_duration_source": self._open_duration_source,
                "setup_done": True,
                "precond": precond}

    def do_work(self, tick: dict, denied_ops: set | None = None) -> Any:
        """执行每轮测试（人工检查步骤 4 起）。

        每轮流程：
          1. remote_open_door（PUT /ISAPI/AccessControl/RemoteControl/door/N）
          2. 睡眠 event_check_delay（2-3s），等待设备将事件写入日志。
          3. 在设备仍在线时，用设备时间窗口查询 3 事件链
             （REMOTE_OPEN + LOCK_OPEN + LOCK_CLOSE）。
          4. （若 run_reboot 且非 skip_reboot）重启设备
          5. 等待 baseline_reboot_duration（pre_loop_setup 中测量）或
             若无基线则回退到 _wait_online() 的 3 阶段探测。
          6. 验证设备在线（get_work_status）。

        事件在步骤 3（每轮重启之前）查询，以便在设备可达时捕获；recover()
        在重启之后无法查询，因为设备可能仍在启动中。
        """
        plan = self.state.get("plan", {}) or {}
        skip_reboot = bool(plan.get("skip_reboot", False))
        denied_ops = denied_ops or set()
        # event_check_delay_adjust 允许 LLM 计划延长延迟（冷启动时事件传播
        # 较慢时），默认 0 表示不调整。
        delay_adjust = float(plan.get("event_check_delay_adjust", 0) or 0)
        op = tick.get("operation") or {"op": "remote_open_door"}
        # 在 do_work 开始时重置每轮时间线。
        self._timeline = []
        self._t0 = time.monotonic()
        stages: Dict[str, Any] = {"op": op.get("op"), "skip_reboot": skip_reboot,
                                  "timeline": self._timeline}
        self._round_online_ok = False
        self._last_offline_cause = None

        # 步骤 1：remote_open_door（被测的协议触发）。
        self._mark("act_start", op=op.get("op"))
        result = self._guarded_adapter_act(op)
        self._mark("act_done", ok=result.ok,
                   error=None if result.ok else result.error)
        stages["act_ok"] = result.ok
        if not result.ok:
            stages["act_error"] = result.error
            # 区分离线成因：循环中门离线（如 remoteDoorControlFailedDoorOffline）
            # 属设备真实故障，由事实独裁判 fail；与前置条件未就绪区分开。
            err = (result.error or "").lower()
            if "offline" in err:
                self._last_offline_cause = "in_loop_device_problem"
                self._chain_stats["in_loop_offline"] += 1
                self._mark("act_door_offline",
                           note="循环过程中门离线=设备问题（事实独裁 fail）")
            self._last_work_stages = stages
            return result

        # 步骤 2：开门后轮询 doorLockStatus 确认「已开 → 已关」全过程，再查询。
        # 映射（用户提供）：doorLockStatus[0]==1 表示开门(解锁)，==0 表示闭锁(关闭)。
        # 轮询（GET）而非固定延迟，可精准等到关闭时刻，确保查询窗口覆盖
        # LOCK_CLOSE(5,22)，且查询终点 <= 当前设备时间（设备事件 API 不接受
        # 未来终点）。
        try:
            open_device_time = self._client.get_time()["Time"]["localTime"]
        except Exception:  # noqa: BLE001
            open_device_time = _now_iso()
        self._open_device_time = open_device_time
        # 以开门时刻为锚点，后向小余量（开门时刻已知，极小即可）；
        # 前向不预设固定余量——轮询确认关闭后即查询（终点=当前设备时间）。
        # delay_adjust 允许 LLM 计划延长后向余量（冷启动事件传播较慢时）。
        backward_buffer = max(self._event_check_delay, 5.0) + delay_adjust
        poll_deadline = time.monotonic() + max(self._open_duration * 3.0 + 5.0, 30.0)
        saw_open = False
        saw_close = False
        self._mark("door_poll_start", open_duration=self._open_duration,
                   backward_buffer=round(backward_buffer, 2))
        while time.monotonic() < poll_deadline:
            try:
                st = self._client.get_work_status().get("AcsWorkStatus", {})
                lock_states = st.get("doorLockStatus") or []
                lock = int(lock_states[0]) if lock_states else 0
            except Exception:  # noqa: BLE001
                lock = -1
            if lock == 1:
                if not saw_open:
                    saw_open = True
                    self._mark("door_open_seen")
            elif lock == 0 and saw_open:
                saw_close = True
                self._mark("door_closed_seen")
                break
            time.sleep(0.5)
        self._mark("door_poll_done", saw_open=saw_open, saw_close=saw_close)

        # 步骤 3：在重启之前（设备在线、可达）查询事件。
        # 使用设备时间窗口以匹配设备事件日志时钟（spec §2.1 A.2）。
        # 窗口以开门时刻为锚点、后向延展 backward_buffer；终点取当前设备时间
        # （轮询已确认关闭，故安全覆盖 LOCK_CLOSE）。应用 baseline serialNo 过滤。
        self._query_events_pre_reboot(open_device_time, backward_buffer)

        # 诊断：若连原始（未过滤基线）的 closed 事件都没有，提示设备可能未
        # 上报 LOCK_CLOSE(5,22) 事件（事件订阅未启用或事件码不符），而非
        # 时序问题。
        if not self._last_events.get("closed"):
            self._mark("warn_closed_missing",
                       note="未查询到 LOCK_CLOSE(5,22) 事件")
            print(f"[诊断] 未检测到门锁关闭事件(LOCK_CLOSE, 5,22)："
                  f"门开启保持={self._open_duration}s，查询窗口已覆盖关闭时刻，"
                  f"但设备未返回关闭事件。请确认设备事件订阅已启用 Door Close，"
                  f"或本设备关闭事件码并非 (5,22)。")

        # 步骤 4-6：重启 + 等待 + 验证（仅在 run_reboot 启用时）。
        if self._run_reboot and not skip_reboot:
            if denied_ops and "reboot" in denied_ops:
                # 按操作鉴权：本轮回退为「不重启」，设备保持在线。
                self._mark("op_denied", op="reboot")
                stages["reboot_denied"] = True
                self._round_online_ok = True
            else:
                self._mark("reboot_start")
                reboot_res = self._guarded_adapter_act({"op": "reboot"})
                self._mark("reboot_done", ok=reboot_res.ok,
                           error=None if reboot_res.ok else reboot_res.error)
                if not reboot_res.ok:
                    stages["reboot_ok"] = False
                    stages["error"] = reboot_res.error
                    self._last_work_stages = stages
                    return reboot_res
                stages["reboot_ok"] = True

                # 步骤 5：等待设备恢复。若 pre_loop_setup 测量得到
                # baseline_reboot_duration，则睡眠该时长（设备重启耗时大致恒定）
                # 然后再验证。否则回退到完整的 3 阶段 _wait_online 探测。
                if self._baseline_reboot_duration > 0:
                    # 渐进式等待：以 baseline 重启耗时为「预期」下限，期间持续
                    # 轮询在线状态（复用 _wait_online）。设备恢复得比预期快则
                    # 提前返回、省去无谓等待；比预期慢则继续探测到
                    # max_recover_timeout，避免漏判离线/恢复。
                    self._mark("reboot_wait_start",
                               expected=round(self._baseline_reboot_duration, 2),
                               mode="baseline_poll")
                    self._wait_online()
                    self._mark("reboot_wait_done",
                               actual=round(time.monotonic() - self._t0, 2))
                else:
                    self._mark("probe_start", interval=self._probe_interval,
                               confirm=self._probe_confirm_count,
                               mode="3phase_probe")
                    online = self._wait_online()
                    self._mark("probe_done", online=online)
                    if not online:
                        from ...multi_agent.adapter import Result
                        stages["online"] = False
                        self._last_work_stages = stages
                        return Result(ok=False, error="device did not come online "
                                      f"within {self._max_recover_timeout}s")

                # 步骤 6：等待后验证设备在线。
                self._mark("verify_online_start")
                try:
                    self._client.get_work_status()
                    online_ok = True
                except Exception:  # noqa: BLE001
                    online_ok = False
                self._mark("verify_online_done", online=online_ok)
                stages["online"] = online_ok
                self._round_online_ok = online_ok
        else:
            # 本轮不重启；步骤 3 之后设备仍在线。
            self._round_online_ok = True

        self._last_work_stages = stages
        return result

    def _query_events_pre_reboot(self, open_iso: str, backward_buffer: float) -> None:
        """在重启之前，用设备时间窗口查询 3 事件链。

        由 do_work() 在设备仍在线时调用。窗口以开门时刻 ``open_iso`` 为锚点、
        向后延展 ``backward_buffer`` 秒（开门时刻已知，后向余量极小即可），
        终点取当前设备时间（do_work 已轮询确认门已关闭，故终点覆盖 LOCK_CLOSE）。
        应用基线 serialNo 过滤器，只统计本轮 remote_open_door 产生的新事件。
        结果存入 self._last_events 供 check() 读取。

        open_iso -- 开门时刻的设备时间（锚点）。
        backward_buffer -- 窗口后向余量（秒）。
        """
        self._mark("query_events_start", open=open_iso,
                   backward=round(backward_buffer, 2))
        start = _iso_seconds_before(open_iso, backward_buffer)
        # 终点取当前设备时间：轮询已确认关闭，故不会早于关闭时刻。
        try:
            device_time = self._client.get_time()["Time"]["localTime"]
        except Exception:  # noqa: BLE001
            device_time = _now_iso()
        end = device_time
        # 防御：若终点晚于当前设备时间（理论上轮询已确认关闭，极少触发），
        # 收敛到当前设备时间（设备事件 API 不接受未来终点）。
        if datetime.fromisoformat(end) > datetime.fromisoformat(device_time):
            end = device_time
        self._mark("query_window", start=start, end=end, open=open_iso)
        try:
            trigger = self._client.query_events(*HikEventCode.REMOTE_OPEN, start, end)
            opened = self._client.query_events(*HikEventCode.LOCK_OPEN, start, end)
            closed = self._client.query_events(*HikEventCode.LOCK_CLOSE, start, end)
        except Exception as exc:  # noqa: BLE001
            self._last_events = {"trigger": [], "opened": [], "closed": []}
            self._recover_error = str(exc)
            self._mark("query_events_done", error=str(exc))
            return
        raw_counts = {"trigger": len(trigger), "opened": len(opened),
                      "closed": len(closed)}
        # 过滤掉基线之前的事件：只统计 serialNo 大于基线的事件，避免上一轮
        # 的残余事件抬高计数。
        if self._baseline_recorded:
            base_serials = self._baseline.get("serialNos", {})
            trigger = [e for e in trigger
                       if int(e.get("serialNo", 0)) > base_serials.get("trigger", 0)]
            opened = [e for e in opened
                      if int(e.get("serialNo", 0)) > base_serials.get("opened", 0)]
            closed = [e for e in closed
                      if int(e.get("serialNo", 0)) > base_serials.get("closed", 0)]
        self._last_events = {"trigger": trigger, "opened": opened, "closed": closed}
        self._mark("query_events_done",
                   raw=raw_counts,
                   filtered={"trigger": len(trigger), "opened": len(opened),
                             "closed": len(closed)})

    def _wait_online(self) -> bool:
        """等待设备重启并恢复在线（spec §4.1）。

        共三个阶段（旧代码跳过了阶段 1，在约 2s 内误判为“在线”——因为重启
        PUT 返回 200 后，设备还会继续提供 HTTP 服务几秒，之后才真正重启）：

        1. 等待设备离线：重启 PUT 立即返回 200，但设备还会继续提供 HTTP 服务
           几秒，之后才真正重启。我们以第一次 get_work_status 失败来确认
           重启已生效。
        2. 等待设备恢复：轮询直到 get_work_status 再次成功（设备重启完成，
           HTTP 服务恢复）。
        3. 确认：要求 probe_confirm_count 次连续成功，以排除启动初期瞬时的
           TCP 绑定（spec §4.1）。

        真实设备重启耗时 30-60s；阶段 1（约 2-5s）+ 阶段 2（约 30-60s）
        + 阶段 3（约 probe_interval*confirm）应落在 max_recover_timeout 内。

        pre_loop_setup() 用它做基准测量；do_work() 在有基线时会改用
        baseline_reboot_duration 睡眠。
        """
        deadline = time.monotonic() + self._max_recover_timeout
        consecutive = 0
        offline_seen = False
        while time.monotonic() < deadline:
            try:
                self._client.get_work_status()
                if not offline_seen:
                    # 阶段 1：设备仍在运行，重启尚未生效。
                    # 持续轮询直到出现失败（设备开始掉线）。
                    time.sleep(self._probe_interval)
                    continue
                # 阶段 3：通过连续成功确认在线
                consecutive += 1
                if consecutive == 1:
                    self._mark("probe_back_online")
                if consecutive >= self._probe_confirm_count:
                    return True
            except Exception:  # noqa: BLE001
                if not offline_seen:
                    # 阶段 1 完成：重启已生效，设备开始掉线
                    offline_seen = True
                    self._mark("probe_offline_seen")
                consecutive = 0
            time.sleep(self._probe_interval)
        return False

    async def recover(self, tick: dict) -> bool:
        """do_work 之后的自愈 + 基线更新。

        事件已在 do_work() 中、每轮重启之前（设备在线时）查询。本方法：
          - 若事件缺失且时间偏差超过阈值，运行 LLM 诊断自愈（在设备时间
            查询窗口下较为少见）。
          - 用本轮事件更新基线 serialNo，供下一轮过滤使用。
          - 返回 do_work() 给出的本轮在线验证状态。
        """
        self._mark("recover_start")
        trigger = self._last_events.get("trigger", [])
        opened = self._last_events.get("opened", [])
        closed = self._last_events.get("closed", [])

        # 自愈：仅当 trigger 缺失且存在诊断内核时。
        # 在设备时间查询窗口 + event_check_delay 下，trigger 很少仅因漂移而
        # 缺失。此分支处理真正缺失的事件（固件 bug、重启后事件日志被清空）。
        if not trigger and self._diagnostic is not None:
            self._mark("heal_diagnose_start")
            skew = self._measure_time_skew()
            env = {"time_skew_seconds": skew,
                   "missing": self._missing_names(trigger, opened, closed),
                   "http_error": getattr(self, "_recover_error", None)}
            decision = self._diagnostic.diagnose(env)
            self._mark("heal_diagnose_done", decision=decision,
                       skew=round(skew, 2))
            if decision == HEAL_TIME_SYNC and skew > self._time_skew_threshold:
                heal_reason: str | None = None
                try:
                    self._client.set_time(_now_iso())
                    self._healed = "time_sync"
                    self._mark("heal_time_sync_done")
                except Exception as exc:  # noqa: BLE001
                    # 决不允许静默吞异常：校时(必要情况 PUT)失败必须留痕，
                    # 否则运维无从判断「为何时间偏差未修正」。
                    self._healed = None
                    heal_reason = f"校时 PUT 失败: {exc}"
                    self._mark("heal_time_sync_failed", reason=heal_reason)
                self._update_baseline_from_events()
                self._mark("recover_done",
                           recovered=self._round_online_ok,
                           healed=self._healed,
                           reason=heal_reason)
                return self._round_online_ok
            self._healed = None
            if decision == HEAL_ABORT:
                self._mark("recover_done", recovered=False, reason="heal_abort")
                return False
        self._healed = None
        self._update_baseline_from_events()
        self._mark("recover_done", recovered=self._round_online_ok)
        return self._round_online_ok

    def check(self, tick: dict) -> dict:
        ev = self._last_events
        # 硬事实（bool）：任一为 False 即判 fail（事实独裁）。
        # remote_open_triggered + lock_opened 是强事件链事实。
        facts = {
            "remote_open_triggered": len(ev["trigger"]) > 0,
            "lock_opened": len(ev["opened"]) > 0,
        }
        # 软事实（非 bool 真值）：lock_closed 是被动事件（门自动关闭），
        # 可能不在查询窗口内发生。按 spec §2.1，它不参与事实独裁，改由
        # Advisor 抬高风险。以字典元数据形式保存，使其可见但不会强制 fail。
        closed_count = len(ev["closed"])
        if closed_count > 0:
            facts["lock_closed"] = {"found": True, "count": closed_count}
        else:
            facts["lock_closed"] = {"found": False, "count": 0}
        if getattr(self, "_healed", None):
            facts["self_healed"] = self._healed  # 非 bool 真值，不会触发 fail
        # 离线成因（软事实）：区分「循环内设备故障」与「前置条件未就绪」。
        if self._last_offline_cause == "in_loop_device_problem":
            facts["door_offline"] = {"in_loop": True, "cause": "device_problem"}
        else:
            facts["door_offline"] = {"in_loop": False}
        # 事件链跨轮统计：累积每轮 3 事件链命中数，便于观测整体稳定性。
        self._chain_stats["rounds"] += 1
        self._chain_stats["trigger"] += len(ev["trigger"])
        self._chain_stats["opened"] += len(ev["opened"])
        self._chain_stats["closed"] += len(ev["closed"])
        self._mark("check_done",
                   remote_open_triggered=facts["remote_open_triggered"],
                   lock_opened=facts["lock_opened"],
                   lock_closed_found=facts["lock_closed"]["found"])
        return facts

    def get_chain_stats(self) -> Dict[str, int]:
        """返回事件链跨轮统计快照（只读副本）。"""
        return dict(self._chain_stats)

    def _measure_time_skew(self) -> float:
        try:
            dev = self._client.get_time()["Time"]["localTime"]
            dev_t = datetime.fromisoformat(dev)
            host_t = datetime.now(dev_t.tzinfo)
            return abs((dev_t - host_t).total_seconds())
        except Exception:  # noqa: BLE001
            return 0.0

    @staticmethod
    def _missing_names(trigger, opened, closed) -> list:
        missing = []
        if not trigger:
            missing.append("remote_open")
        if not opened:
            missing.append("lock_open")
        if not closed:
            missing.append("lock_close")
        return missing


__all__ = ["HikvisionWorker"]
