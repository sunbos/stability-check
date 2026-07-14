"""Telemetry — structured logging + pluggable sinks.

Emits trace/metric records both to local sinks (e.g. PrintSink) and onto the bus
under ``harness/metric/<name>`` (metrics) and ``harness/trace`` (traces), so any
observer agent can consume them without coupling to this module.
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional

from .bus import EventBus


class Sink:
    """Base class for telemetry sinks. Override ``emit``."""

    def emit(self, record: Dict[str, Any]) -> None:
        raise NotImplementedError


class PrintSink(Sink):
    """Default sink: pretty JSON to stdout."""

    def emit(self, record: Dict[str, Any]) -> None:
        print(json.dumps(record, ensure_ascii=False))


class MemorySink(Sink):
    """In-memory sink — keeps every record in a list. Handy for tests/assertions.

    Records are plain dicts as produced by ``Telemetry._emit``. Use ``get``/``count``
    to filter, or ``clear`` between cases.
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
    """Sink that discards everything. Useful to silence telemetry in benchmarks."""

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
                self._log.exception("sink error")
        if self.bus is not None:
            topic = f"harness/metric/{name}" if kind == "metric" else "harness/trace"
            self.bus.publish(topic, record)
        return record


__all__ = ["Telemetry", "Sink", "PrintSink", "MemorySink", "NullSink"]
