"""Telemetry —— 结构化日志 + 可插拔的输出槽（sink）。

既向本地输出槽（例如 PrintSink）发出 trace/metric 记录，也向总线上的
``harness/metric/<name>``（指标）与 ``harness/trace``（追踪）发出，这样任何
观察者智能体都能消费它们，而无需与本模块耦合。
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional

from .bus import EventBus


class Sink:
    """遥测输出槽的基类。覆盖 ``emit``。"""

    def emit(self, record: Dict[str, Any]) -> None:
        raise NotImplementedError


class PrintSink(Sink):
    """默认输出槽：将记录以漂亮的 JSON 形式打印到 stdout。"""

    def emit(self, record: Dict[str, Any]) -> None:
        print(json.dumps(record, ensure_ascii=False))


class MemorySink(Sink):
    """内存输出槽 —— 将每条记录保存在一个列表中。便于测试/断言使用。

    记录是 ``Telemetry._emit`` 产生的普通字典。可用 ``get``/``count``
    进行过滤，或在用例之间用 ``clear`` 清空。
    """

    def __init__(self, maxlen: Optional[int] = None) -> None:
        self.records: List[Dict[str, Any]] = []
        self._maxlen = maxlen

    def emit(self, record: Dict[str, Any]) -> None:
        self.records.append(record)
        if self._maxlen and len(self.records) > self._maxlen:
            self.records.pop(0)

    def clear(self) -> None:
        self.records.clear()

    def get(self, name: Optional[str] = None, kind: Optional[str] = None) -> List[Dict[str, Any]]:
        out = self.records
        if name is not None:
            out = [r for r in out if r.get("name") == name]
        if kind is not None:
            out = [r for r in out if r.get("kind") == kind]
        return out

    def count(self, name: Optional[str] = None) -> int:
        return len(self.get(name))


class NullSink(Sink):
    """丢弃一切的输出槽。便于在基准测试中屏蔽遥测输出。"""

    def emit(self, record: Dict[str, Any]) -> None:
        return None


class Telemetry:
    def __init__(
        self,
        bus: Optional[EventBus] = None,
        sinks: Optional[List[Sink]] = None,
    ) -> None:
        self.bus = bus
        self._sinks = sinks if sinks is not None else [PrintSink()]
        self._log = logging.getLogger("stability_harness_loop_multiagent.telemetry")

    def trace(self, name: str, **fields: Any) -> Dict[str, Any]:
        return self._emit("trace", name, fields)

    def metric(self, name: str, value: float, **fields: Any) -> Dict[str, Any]:
        return self._emit("metric", name, {**fields, "value": value})

    def _emit(self, kind: str, name: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        record = {"kind": kind, "name": name, "ts": time.time(), **fields}
        for sink in self._sinks:
            try:
                sink.emit(record)
            except Exception:  # noqa: BLE001
                self._log.exception("sink 出错")
        if self.bus is not None:
            topic = f"harness/metric/{name}" if kind == "metric" else "harness/trace"
            self.bus.publish(topic, record)
        return record


__all__ = ["Telemetry", "Sink", "PrintSink", "MemorySink", "NullSink"]
