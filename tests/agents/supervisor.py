"""拷机编排：Supervisor 主循环。

仅使用标准库 asyncio / time。严格依赖同目录下的：
  - reboot_agent.run_reboot
  - event_check_agent.check_reboot_event
  - status_check_agent.check_status
  - config（RunConfig / Baseline / RoundResult）
  - strategy.Strategy（extra_status_asserts）
  - report.Reporter（record / abort / aborted / summary）

注意：本模块及其依赖均以绝对导入引用同级模块，因此 tests/agents 需位于
sys.path（由 conftest.py / test_burnin.py 负责加入）。
"""

import asyncio
import time

import reboot_agent
import event_check_agent
import status_check_agent
import config
import strategy as strategy_module
import report as report_module


# 恢复轮询的轮询间隔（秒）。config 未提供该字段，作为 supervisor 内部常量。
POLL_INTERVAL = 5.0


class Supervisor:
    """稳定性拷机主循环编排器。"""

    def __init__(self, cfg, client, baseline, strategy, reporter):
        self.cfg = cfg
        self.client = client
        self.baseline = baseline
        self.strategy = strategy
        self.reporter = reporter

    # ---- 内部工具 ----------------------------------------------------------
    def _record_round(
        self,
        *,
        round_no,
        t_reboot,
        t_recover,
        recover_time,
        reboot_event_found,
        status_changed,
        status_diff,
        passed,
        error,
    ) -> dict:
        result = {
            "round_no": round_no,
            "t_reboot": t_reboot,
            "t_recover": t_recover,
            "recover_time": recover_time,
            "reboot_event_found": reboot_event_found,
            "status_changed": status_changed,
            "status_diff": status_diff,
            "passed": passed,
            "error": error,
        }
        self.reporter.record(result)
        return result

    def _exceeded(self, total_failures, consecutive_failures):
        cfg = self.cfg
        if cfg.fail_threshold > 0 and total_failures >= cfg.fail_threshold:
            return True, (
                f"累计失败 {total_failures} >= 阈值 {cfg.fail_threshold}"
            )
        if cfg.fail_consecutive > 0 and consecutive_failures >= cfg.fail_consecutive:
            return True, (
                f"连续失败 {consecutive_failures} "
                f">= 上限 {cfg.fail_consecutive}"
            )
        return False, ""

    # ---- 主循环 ------------------------------------------------------------
    async def run(self):
        round_no = 1
        total_failures = 0
        consecutive_failures = 0
        consecutive_reboots = 0
        start_time = time.time()

        while True:
            # 终止条件：达到最大轮次或最大运行时长
            if self.cfg.max_rounds > 0 and round_no > self.cfg.max_rounds:
                break
            if (
                self.cfg.max_duration > 0
                and (time.time() - start_time) > self.cfg.max_duration
            ):
                break

            t_reboot = time.time()
            reboot_ok = True
            recovered = True
            reboot_event_found = False
            status_ok = False
            status_diff = {}
            error = None

            # 1) 发起重启（同步；异常则记失败并 continue）
            try:
                reboot_agent.run_reboot(self.client)
            except Exception as e:  # noqa: BLE001 - 统一吞掉并标记失败
                reboot_ok = False
                error = f"重启失败: {e}"
                self._record_round(
                    round_no=round_no,
                    t_reboot=t_reboot,
                    t_recover=None,
                    recover_time=None,
                    reboot_event_found=False,
                    status_changed=False,
                    status_diff={},
                    passed=False,
                    error=error,
                )
                total_failures += 1
                consecutive_failures += 1
                consecutive_reboots = 0
                exceeded, reason = self._exceeded(total_failures, consecutive_failures)
                if exceeded:
                    self.reporter.abort(reason)
                    break
                round_no += 1
                continue

            # 2) 等待设备经历 离线->恢复 的完整重启周期。
            #    不能只判断“当前在线”：重启命令发出后设备往往仍在线片刻，
            #    首次轮询会误判已恢复（recover_time≈0）。必须观察到先掉线再上线。
            saw_down = False
            t_recover = None
            poll_deadline = t_reboot + self.cfg.recover_timeout
            while time.time() < poll_deadline:
                try:
                    self.client.get_work_status()
                    if saw_down:
                        t_recover = time.time()
                        break
                    # 仍处于重启前的在线态，继续等待其掉线
                except Exception:  # noqa: BLE001 - 设备已掉线
                    saw_down = True
                await asyncio.sleep(POLL_INTERVAL)

            if t_recover is None:
                error = (
                    "设备未在恢复超时内完成重启周期（离线->上线）"
                )
                self._record_round(
                    round_no=round_no,
                    t_reboot=t_reboot,
                    t_recover=None,
                    recover_time=None,
                    reboot_event_found=False,
                    status_changed=False,
                    status_diff={},
                    passed=False,
                    error=error,
                )
                total_failures += 1
                consecutive_failures += 1
                consecutive_reboots = 0
                exceeded, reason = self._exceeded(total_failures, consecutive_failures)
                if exceeded:
                    self.reporter.abort(reason)
                    break
                round_no += 1
                continue

            # 3) 恢复后并发核对重启事件与状态
            try:
                reboot_event_found, status_result = await asyncio.gather(
                    asyncio.to_thread(
                        event_check_agent.check_reboot_event,
                        self.client,
                        t_reboot,
                        t_recover,
                        self.cfg.event_window,
                    ),
                    asyncio.to_thread(
                        status_check_agent.check_status,
                        self.client,
                        self.baseline,
                        self.strategy.extra_status_asserts(
                            round_no, consecutive_reboots
                        ),
                    ),
                )
                status_ok, status_diff = status_result
            except Exception as e:  # noqa: BLE001 - 检查异常记为本轮失败
                error = f"恢复后检查失败: {e}"
                reboot_event_found = False
                status_ok = False
                status_diff = {}

            status_changed = len(status_diff) > 0
            recover_time = t_recover - t_reboot
            passed = bool(reboot_event_found and status_ok)

            print(
                f"[拷机] 第 {round_no} 轮 通过={passed} "
                f"事件={reboot_event_found} 状态偏移={status_changed} "
                f"差异={status_diff} 恢复耗时={recover_time:.1f}秒 错误={error}",
                flush=True,
            )

            self._record_round(
                round_no=round_no,
                t_reboot=t_reboot,
                t_recover=t_recover,
                recover_time=recover_time,
                reboot_event_found=reboot_event_found,
                status_changed=status_changed,
                status_diff=status_diff,
                passed=passed,
                error=error,
            )

            # 4) 失败计数与自适应
            if passed:
                consecutive_failures = 0
                consecutive_reboots += 1
            else:
                total_failures += 1
                consecutive_failures += 1
                consecutive_reboots = 0

            exceeded, reason = self._exceeded(total_failures, consecutive_failures)
            if exceeded:
                self.reporter.abort(reason)
                break

            # 5) 自适应间隔后进入下一轮
            interval = max(
                self.cfg.interval_min,
                min(
                    self.cfg.interval_max,
                    recover_time * self.cfg.k + self.cfg.base_interval,
                ),
            )
            round_no += 1
            await asyncio.sleep(interval)

        # 循环结束：调用 reporter 收尾（若支持 finalize）
        finalize = getattr(self.reporter, "finalize", None)
        if callable(finalize):
            finalize()
