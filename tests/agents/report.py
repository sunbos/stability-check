"""稳定性测试结果汇总与告警（仅使用标准库）。

提供：
  - Reporter  记录各轮 RoundResult、标记中断、输出汇总与告警
"""

import time


class Reporter:
    def __init__(self):
        self.results = []
        self._aborted = False
        self._abort_reason = ""
        self._start_time = time.time()

    def record(self, result: dict):
        """追加一轮 RoundResult（dict 形式）。"""
        self.results.append(result)

    def abort(self, reason: str):
        """标记测试流程被中断并记录原因。"""
        self._aborted = True
        self._abort_reason = reason

    @property
    def aborted(self) -> bool:
        return self._aborted

    def alert(self, msg: str):
        """打印告警，并预留 webhook 钩子。"""
        print(f"[ALERT] {msg}")
        self.send_webhook(msg)

    def send_webhook(self, msg):
        """预留 webhook 发送钩子，默认不做真实发送。"""
        pass

    def summary(self) -> dict:
        """返回汇总统计。

        avg_recover_time / max_recover_time 仅统计 recover_time 有效的轮次。
        """
        total = len(self.results)
        passed = sum(1 for r in self.results if r.get("passed"))
        failed = sum(1 for r in self.results if not r.get("passed"))

        recover_times = [
            r.get("recover_time")
            for r in self.results
            if r.get("recover_time") is not None
        ]
        avg_recover_time = (
            sum(recover_times) / len(recover_times) if recover_times else None
        )
        max_recover_time = max(recover_times) if recover_times else None

        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "aborted": self._aborted,
            "reason": self._abort_reason,
            "avg_recover_time": avg_recover_time,
            "max_recover_time": max_recover_time,
        }
