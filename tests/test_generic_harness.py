"""通用装配模板的回归测试（结构锁定，非正式设备验证）。

本文件只证明 ``examples/generic_harness.run_generic`` 的装配接线正确、可端到端
运行：EventBus + Telemetry + Watchdog + Runtime（监督看门狗）+ ControlLoop +
通用 Worker/Advisor/Observer，可选挂载 Governance/Verify 网关。

注意：这里用 ``GenericTargetAdapter``（内存计数器）占位，属于**结构/蓝图**级别
的回归——它锁死的是「基座接线」，不是设备稳定性本身。Watchdog 死锁中止、
Runtime 监督重启这类安全网，最终必须在**真实设备**上做集成验证（见
examples/hikvision_real_env.py 与 108 条场景 YAML），本测试不替代那一步。

``test_generic_harness_real_device_injection`` 额外锁定「真实设备模式」的接线：
注入一个阻塞型（真实设备形态）的 ``TargetAdapter``，验证 ``RealDeviceWorker``
的 ``asyncio.to_thread`` 包裹路径能端到端产出裁决、事件循环不被阻塞。
"""

import asyncio
import time

import pytest

from stability_harness_loop_multiagent.examples.generic_harness import (
    _instantiate_target,
    assert_failing_fact,
    assert_healthy,
    read_scenario_env,
    run_generic,
    run_generic_env,
)
from stability_harness_loop_multiagent.business.hikvision.adapter import (
    HikvisionAdapter,
    HikvisionAdapterFactory,
)
from stability_harness_loop_multiagent.multi_agent.adapter import (
    Event,
    Result,
    State,
)


class _BlockingAdapter:
    """模拟真实设备适配器：act/observe 阻塞（即便只有几毫秒），用于验证
    RealDeviceWorker 的 to_thread 包裹路径在真实设备模式下正确接线——证明
    阻塞调用被挪到工作线程、事件循环不被卡死。"""

    def __init__(self) -> None:
        self.calls = 0

    def act(self, operation) -> Result:
        time.sleep(0.01)  # 模拟阻塞型设备 IO
        self.calls += 1
        return Result(ok=True, data={"op": operation, "calls": self.calls})

    def observe(self) -> State:
        return State(snapshot={"up": True, "calls": self.calls})

    def events(self, since: float):
        return [Event(kind="acted", payload={"calls": self.calls}, ts=since)]


@pytest.mark.asyncio
async def test_generic_harness_healthy_with_governance_and_verify():
    """健康路径 + opt-in 治理/校验网关：全 pass、轮数与执行一致。"""
    result = await run_generic(
        fail=False, max_rounds=5, enable_governance=True, enable_verify=True
    )
    assert_healthy(result)
    assert "governance" in result  # 治理网关确实被挂载
    assert "verifier" in result    # 校验网关确实被挂载
    # 看门狗被 Runtime 监督且循环正常结束 -> Runtime 已优雅关停（无残留任务异常）。


@pytest.mark.asyncio
async def test_generic_harness_failing_fact_tyranny():
    """事实独裁：注入失败事实 -> 强制 fail，即使 Advisor 投低风险。"""
    result = await run_generic(fail=True, max_rounds=5)
    assert_failing_fact(result)


@pytest.mark.asyncio
async def test_generic_harness_real_device_injection():
    """真实设备模式：注入外部阻塞型 TargetAdapter，走 to_thread 路径，端到端产出裁决。

    锁死「真实设备模式」接线：Worker 必须用 asyncio.to_thread 包裹阻塞调用，
    否则事件循环被卡死会导致 ControlLoop 超时安全网/看门狗失效（spec §3.1.3）。
    """
    adapter = _BlockingAdapter()
    result = await run_generic(
        target_adapter=adapter, max_rounds=3,
        enable_governance=True, enable_verify=True,
        # 测试用短超时：仍走 RealDeviceWorker 的 to_thread 路径，只是不空等。
        device_op_timeout=0.1, vote_timeout=0.1,
        recover_timeout=0.1, check_timeout=0.1, round_interval=0.0,
    )
    ctx = result["ctx"]
    assert ctx.round_count >= 1, "真实设备模式没有产生任何轮次"
    assert result["loop"].verdict is not None, "真实设备模式没有设置权威裁决"
    history = ctx.snapshot().round_history
    assert all(r.verdict == "pass" for r in history), (
        f"健康设备应当只产生 'pass' 裁决：{[r.verdict for r in history]}"
    )
    # 阻塞型适配器被真实调用了 round_count 次（to_thread 路径确实执行了 do_work）。
    assert adapter.calls == ctx.round_count, (
        f"Worker 执行了 {adapter.calls} 次，但运行了 {ctx.round_count} 轮"
    )
    assert result["observer"].seen, "Observer 没有收到事件（真实设备模式总线端到端可用）"


@pytest.mark.asyncio
async def test_generic_harness_strategy_observation():
    """自然语言策略能力：传入「观察 XXX」，Worker 写出观察事实、Observer 广播闭环。

    锁死「前端写提示语 -> 系统观察」的闭环：策略被归一为观察项，落到 Worker 事实
    （裁决/遥测可追溯）、Observer（每轮广播 ``agent/scribe/observe``）、总线
    （``target/strategy`` 供前端订阅）。
    """
    strategy = "观察设备温度在重启后是否回落到正常区间"
    result = await run_generic(
        fail=False, max_rounds=3, enable_governance=True, enable_verify=True,
        strategy=strategy,
    )
    directive = result["strategy"]
    assert "设备温度在重启后是否回落到正常区间" in directive.observe, (
        f"策略未解析出观察目标：{directive.observe}"
    )
    # Observer 也拿到了同一指令（可用于每轮广播观察事实）。
    assert "设备温度在重启后是否回落到正常区间" in result["observer"].directives.observe

    topics = [t for t, _ in result["observer"].seen]
    assert "target/strategy" in topics, "策略未发布到总线（前端无法订阅观察项）"

    # Worker 把观察项写进了每轮事实（裁决记录/遥测可追溯「观察了什么」）。
    history = result["ctx"].snapshot().round_history
    assert history, "没有产生轮次记录"
    for r in history:
        observing = r.facts.get("observing", [])
        assert "设备温度在重启后是否回落到正常区间" in observing, (
            f"轮次 {r.round} 事实未包含观察项：{r.facts}"
        )


@pytest.mark.asyncio
async def test_generic_harness_env_driven_config(monkeypatch):
    """os.env 自定义参数通道：前端经 os.environ 透传轮数与策略，pytest 入口生效。

    锁死「前端 -> os.environ -> run_generic_env -> run_generic」这条配置链路：
    STABILITY_ROUNDS 控制轮数、STABILITY_STRATEGY 注入自然语言策略。
    """
    for var in (
        "STABILITY_REAL_TARGET", "STABILITY_ROUNDS", "STABILITY_STRATEGY",
        "STABILITY_GOVERNANCE", "STABILITY_VERIFY", "STABILITY_REAL_DEVICE",
    ):
        monkeypatch.delenv(var, raising=False)
    # 无场景变量时 read_scenario_env 必须返回空（_main 据此回退默认演示）。
    assert read_scenario_env() == {}

    monkeypatch.setenv("STABILITY_ROUNDS", "3")
    monkeypatch.setenv("STABILITY_STRATEGY", "观察门禁在掉电后能否自动恢复")
    monkeypatch.setenv("STABILITY_GOVERNANCE", "1")
    monkeypatch.setenv("STABILITY_VERIFY", "1")

    result = await run_generic_env()
    ctx = result["ctx"]
    assert ctx.round_count == 3, f"STABILITY_ROUNDS 未生效：{ctx.round_count}"
    assert "门禁在掉电后能否自动恢复" in result["strategy"].observe, (
        f"STABILITY_STRATEGY 未生效：{result['strategy'].observe}"
    )
    assert "governance" in result and "verifier" in result, "网关开关未生效"


def test_read_scenario_env_device_config(monkeypatch):
    """设备连接信息（host/user/pass）经 os.env 收集为 _device_config。"""
    monkeypatch.setenv(
        "STABILITY_DEVICE_CONFIG", '{"host":"10.0.0.1","pass":"secret"}'
    )
    monkeypatch.setenv("STABILITY_DEVICE_USER", "admin")
    cfg = read_scenario_env()
    dev = cfg.get("_device_config", {})
    assert dev.get("host") == "10.0.0.1"
    assert dev.get("user") == "admin"
    assert dev.get("pass") == "secret"


def test_read_scenario_env_device_config_three_fields(monkeypatch):
    """前端 3 输入框（IP/用户名/密码）经 os.env 收集为 _device_config。"""
    for var in ("STABILITY_DEVICE_CONFIG", "STABILITY_DEVICE_IP",
                "STABILITY_DEVICE_USERNAME", "STABILITY_DEVICE_PASSWORD",
                "STABILITY_DEVICE_HOST", "STABILITY_DEVICE_USER"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("STABILITY_DEVICE_IP", "10.0.0.1")
    monkeypatch.setenv("STABILITY_DEVICE_USERNAME", "admin")
    monkeypatch.setenv("STABILITY_DEVICE_PASSWORD", "secret")
    cfg = read_scenario_env()
    dev = cfg.get("_device_config", {})
    assert dev.get("ip") == "10.0.0.1"
    assert dev.get("username") == "admin"
    assert dev.get("password") == "secret"


def test_hikvision_adapter_factory_three_fields():
    """海康适配器工厂：前端 3 输入框（IP/用户名/密码）直接产出 HikvisionAdapter。

    锁死「前端透传 IP/用户名/密码 -> 工厂归一 -> 真实设备适配器」这条接缝：
    关键字直传与 device_config dict 两种入口等价，且字段别名（host/user/pass）
    同样可归一。
    """
    adapter = HikvisionAdapterFactory.create(
        ip="10.0.0.1", username="admin", password="secret", port=80
    )
    assert isinstance(adapter, HikvisionAdapter)
    assert adapter._client._base == "http://10.0.0.1:80"
    assert adapter._client._user == "admin"
    assert adapter._client._password == "secret"

    # device_config dict 等价路径（含 host/user/pass 别名归一）。
    adapter2 = HikvisionAdapterFactory.create(
        {"host": "10.0.0.2", "user": "root", "pass": "p2"}
    )
    assert isinstance(adapter2, HikvisionAdapter)
    assert adapter2._client._base == "http://10.0.0.2:80"
    assert adapter2._client._user == "root"
    assert adapter2._client._password == "p2"


def test_hikvision_adapter_device_config_injection():
    """HikvisionAdapter(device_config=...) 与通用入口 STABILITY_REAL_TARGET 接线一致。

    验证通用入口 ``STABILITY_REAL_TARGET=...:HikvisionAdapter`` + 3 输入框可被
    ``_instantiate_target`` 离线实例化（不触达网络，仅构造 client）。
    """
    adapter = _instantiate_target(
        "stability_harness_loop_multiagent.business.hikvision.adapter:HikvisionAdapter",
        device_config={"ip": "10.0.0.1", "username": "admin", "password": "x"},
    )
    assert isinstance(adapter, HikvisionAdapter)
    assert adapter._client._base == "http://10.0.0.1:80"


@pytest.mark.asyncio
async def test_generic_harness_device_config_surfaced():
    """设备信息在 run 中脱敏展示并广播到总线（前端可订阅 target/device）。"""
    result = await run_generic(
        fail=False, max_rounds=2,
        device_config={"host": "10.0.0.1", "user": "admin", "pass": "secret"},
    )
    dc = result["device_config"]
    assert dc["host"] == "10.0.0.1"
    assert dc["user"] == "admin"
    assert dc["pass"] == "***", "密钥必须脱敏，不可回显原文"
    topics = [t for t, _ in result["observer"].seen]
    assert "target/device" in topics, "设备信息未广播到总线"


@pytest.mark.asyncio
async def test_generic_harness_operation_and_act_ok_fact():
    """可配置操作 + act_ok 事实：真实设备模式下 operation 透传到 adapter。

    锁死「前端经 STABILITY_OPERATION 指定操作 -> Worker 真正执行该操作」：
    RealDeviceWorker 把操作名传给 adapter.act，并把操作结果写入 act_ok 事实
    （失败 -> 事实独裁判 fail）。这里用阻塞型占位适配器断言 operation 透传。
    """
    seen_ops = []

    class _OpAdapter:
        def __init__(self) -> None:
            self.calls = 0

        def act(self, operation):
            seen_ops.append(operation)
            self.calls += 1
            return Result(ok=True, data={"op": operation})

        def observe(self):
            return State(snapshot={"up": True})

        def events(self, since):
            return []

    adapter = _OpAdapter()
    result = await run_generic(
        target_adapter=adapter, max_rounds=2, operation="remote_open_door",
        device_op_timeout=0.1, vote_timeout=0.1,
        recover_timeout=0.1, check_timeout=0.1, round_interval=0.0,
    )
    ctx = result["ctx"]
    assert ctx.round_count == 2
    assert seen_ops == ["remote_open_door", "remote_open_door"], (
        f"operation 未透传到 adapter：{seen_ops}"
    )
    # 操作成功 -> act_ok 为真 -> 健康 pass。
    history = ctx.snapshot().round_history
    assert all(r.verdict == "pass" for r in history), history
    for r in history:
        assert r.facts.get("act_ok") is True, f"act_ok 应为真：{r.facts}"


if __name__ == "__main__":
    asyncio.run(run_generic(fail=False, max_rounds=3))
