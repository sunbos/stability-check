"""共享上下文 + 任务清单（小组白板）。

RunContext
----------
持有整场拷机的共享状态：基线快照 baseline、策略文本 strategy_text、
每轮记录 round_history、运行日志 log。在单一事件循环内直接操作即可
（同一循环内无需额外加锁）；提供 append_log / record_round。

TaskBoard
---------
所有 agent 共同的任务清单（白板）。协调者维护它；agent 通过总线或直接读
ctx.board 获取/更新共同清单。任务状态：'pending' | 'doing' | 'done' | 'failed'。

设计说明
--------
RunContext + TaskBoard 即“共同上下文 / 共享清单”。所有 agent 经协调者读写，
agent 之间不直接共享可变状态，避免并发不一致。

仅依赖标准库 dataclasses，无第三方依赖。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Task:
    """清单中的一项任务。"""

    name: str
    status: str = "pending"  # 'pending' | 'doing' | 'done' | 'failed'
    result: Optional[dict] = None


class RunContext:
    """整场拷机的共享上下文（小组白板的数据面）。"""

    def __init__(
        self,
        baseline: Optional[dict] = None,
        strategy_text: str = "",
        round_history: Optional[list] = None,
        log: Optional[list] = None,
    ) -> None:
        self.baseline: dict = baseline if baseline is not None else {}
        self.strategy_text: str = strategy_text
        self.round_history: list = round_history if round_history is not None else []
        self.log: list = log if log is not None else []
        # 共同的任务清单，协调者维护，agent 直接读取
        self.board = TaskBoard()

    # ------------------------------------------------------------------ #
    # 日志 / 轮次记录（单事件循环内直接操作即可，async-safe by design）
    # ------------------------------------------------------------------ #
    def append_log(self, entry: str) -> None:
        """追加一条日志。返回其索引，便于引用。"""
        self.log.append(entry)
        return len(self.log) - 1

    def record_round(self, record: dict) -> None:
        """记录一轮的结果（追加到 round_history）。"""
        self.round_history.append(record)

    # ------------------------------------------------------------------ #
    # 便捷访问
    # ------------------------------------------------------------------ #
    def set_baseline(self, baseline: dict) -> None:
        self.baseline = baseline

    def set_strategy(self, strategy_text: str) -> None:
        self.strategy_text = strategy_text


class TaskBoard:
    """所有 agent 共同的任务清单（白板）。"""

    def __init__(self) -> None:
        self.tasks: list = []

    def add(self, task: Task) -> None:
        """添加一个任务。若同名任务已存在则覆盖。"""
        for i, t in enumerate(self.tasks):
            if t.name == task.name:
                self.tasks[i] = task
                return
        self.tasks.append(task)

    def mark(self, name: str, status: str, result: Optional[dict] = None) -> bool:
        """把名为 name 的任务标记为 status（可附带 result）。返回是否找到该任务。"""
        for t in self.tasks:
            if t.name == name:
                t.status = status
                t.result = result
                return True
        return False

    def get_pending(self, role: Optional[str] = None) -> list:
        """返回待处理（status == 'pending'）的任务列表。

        role 指定时，仅返回名称以 'role/' 开头的任务（便于按角色过滤）。
        """
        out = [t for t in self.tasks if t.status == "pending"]
        if role is not None:
            out = [t for t in out if t.name.startswith(role + "/")]
        return out

    def snapshot(self) -> list:
        """返回全部任务的 dict 快照列表（供日志/上报使用）。"""
        return [
            {"name": t.name, "status": t.status, "result": t.result}
            for t in self.tasks
        ]
