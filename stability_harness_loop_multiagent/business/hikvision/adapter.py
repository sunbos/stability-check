"""HikvisionAdapter: sync TargetAdapter over HikvisionClient.

Implements the TargetAdapter protocol (act/observe/events). Sync because
the protocol is sync; Worker wraps with asyncio.to_thread for parallelism.
"""

from typing import Any, List

from ...multi_agent.adapter import Event, Result, State


class HikvisionAdapter:
    """Sync TargetAdapter implementation for Hikvision door access."""

    def __init__(self, client) -> None:
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


__all__ = ["HikvisionAdapter"]
