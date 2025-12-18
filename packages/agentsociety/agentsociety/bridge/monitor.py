"""Utilities for evaluating bridge maintenance workflows.

This module keeps lightweight, in-memory records of bridge inspections,
interventions, and work orders so that the simulator can expose summary
metrics and visualization overlays.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from statistics import mean
from typing import Any, Iterable, Mapping

from ..tools.bridge_tools import BridgeRecord


def _parse_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _serialize_date(value: object) -> str | None:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if value is None:
        return None
    return str(value)


def _extract_bridge_id(record: Mapping[str, Any]) -> str:
    for key in ("bridge_id", "structure_id", "id", "structure_number", "structure_num"):
        candidate = record.get(key)
        if candidate not in (None, ""):
            return str(candidate)
    return "unknown"


def _extract_coords(record: Mapping[str, Any]) -> tuple[float | None, float | None]:
    lng = _parse_float(record.get("longitude") or record.get("lng") or record.get("lon"))
    lat = _parse_float(record.get("latitude") or record.get("lat"))
    return lng, lat


@dataclass
class BridgeStatusEntry:
    """Snapshot used for visualization overlays."""

    bridge_id: str
    name: str | None
    priority: str | None
    risk: str | None
    status: str
    work_order_status: str | None
    action: str | None
    due_date: str | None
    days_overdue: int | None
    lng: float | None
    lat: float | None
    last_update: dict[str, Any] = field(default_factory=dict)


class BridgeMaintenanceMonitor:
    """Collects evaluation signals for bridge maintenance runs."""

    def __init__(self) -> None:
        self._backlog_history: list[tuple[int, int]] = []
        self._pending_backlog_step: dict[str, int] = {}
        self._response_times: list[int] = []
        self._mitigated_bridges: set[str] = set()
        self._interventions: list[dict[str, Any]] = []
        self._inspections: list[dict[str, Any]] = []
        self._statuses: dict[str, BridgeStatusEntry] = {}

    def reset(self) -> None:
        """Clear all cached bridge metrics and logs."""

        self._backlog_history.clear()
        self._pending_backlog_step.clear()
        self._response_times.clear()
        self._mitigated_bridges.clear()
        self._interventions.clear()
        self._inspections.clear()
        self._statuses.clear()

    def record_backlog(
        self,
        backlog: Iterable[Mapping[str, Any]],
        *,
        day: int,
        t: float,
        step: int,
    ) -> None:
        """Log a backlog snapshot produced by schedulers/inspectors."""

        backlog_list = list(backlog)
        self._backlog_history.append((step, len(backlog_list)))
        for entry in backlog_list:
            bridge: BridgeRecord = entry.get("bridge", {})  # type: ignore[assignment]
            bridge_id = _extract_bridge_id(bridge)
            due_date = _serialize_date(entry.get("due_date"))
            days_overdue = _parse_int(entry.get("days_overdue"))
            risk_level = str(bridge.get("risk_level") or bridge.get("risk") or "").lower() or None
            name = bridge.get("name") or bridge.get("bridge_name")
            lng, lat = _extract_coords(bridge)

            self._inspections.append(
                {
                    "bridge_id": bridge_id,
                    "day": day,
                    "t": t,
                    "due_date": due_date,
                    "days_overdue": days_overdue,
                    "risk": risk_level,
                }
            )

            self._pending_backlog_step[bridge_id] = step
            priority = "critical" if days_overdue is not None and days_overdue > 0 else "scheduled"
            status = self._statuses.get(
                bridge_id,
                BridgeStatusEntry(
                    bridge_id=bridge_id,
                    name=name if isinstance(name, str) else None,
                    priority=priority,
                    risk=risk_level,
                    status="inspection_due",
                    work_order_status=None,
                    action=None,
                    due_date=due_date,
                    days_overdue=days_overdue,
                    lng=lng,
                    lat=lat,
                    last_update={},
                ),
            )

            status.priority = priority
            status.risk = risk_level or status.risk
            status.status = "inspection_due" if (days_overdue or 0) >= 0 else "scheduled"
            status.due_date = due_date
            status.days_overdue = days_overdue
            status.lng = status.lng or lng
            status.lat = status.lat or lat
            status.last_update = {"day": day, "t": t, "step": step}
            self._statuses[bridge_id] = status

    def record_inspection_findings(
        self,
        proposals: Iterable[Mapping[str, Any]],
        *,
        day: int,
        t: float,
        step: int,
    ) -> None:
        """Log the outcomes/triage results from inspection reasoning."""

        for proposal in proposals:
            bridge: BridgeRecord = proposal.get("bridge", {})  # type: ignore[assignment]
            bridge_id = _extract_bridge_id(bridge)
            priority = proposal.get("priority")
            risk_level = proposal.get("risk")
            name = bridge.get("name") or bridge.get("bridge_name")
            lng, lat = _extract_coords(bridge)

            self._inspections.append(
                {
                    "bridge_id": bridge_id,
                    "priority": priority,
                    "risk": risk_level,
                    "day": day,
                    "t": t,
                }
            )

            status = self._statuses.get(
                bridge_id,
                BridgeStatusEntry(
                    bridge_id=bridge_id,
                    name=name if isinstance(name, str) else None,
                    priority=str(priority) if priority else None,
                    risk=str(risk_level) if risk_level else None,
                    status="triaged",
                    work_order_status=None,
                    action=None,
                    due_date=None,
                    days_overdue=None,
                    lng=lng,
                    lat=lat,
                    last_update={},
                ),
            )

            status.priority = str(priority) if priority else status.priority
            status.risk = str(risk_level) if risk_level else status.risk
            status.status = "triaged"
            status.lng = status.lng or lng
            status.lat = status.lat or lat
            status.last_update = {"day": day, "t": t, "step": step}
            self._statuses[bridge_id] = status

    def record_intervention(
        self,
        bridge: Mapping[str, Any],
        *,
        priority: str | None,
        action: str | None,
        assigned_to: int | None,
        day: int,
        t: float,
        step: int,
    ) -> None:
        """Log an intervention/work order dispatch."""

        bridge_id = _extract_bridge_id(bridge)
        response_time = None
        if bridge_id in self._pending_backlog_step:
            response_time = max(0, step - self._pending_backlog_step[bridge_id])
            self._response_times.append(response_time)
            del self._pending_backlog_step[bridge_id]

        lng, lat = _extract_coords(bridge)
        risk_level = bridge.get("risk_level") or bridge.get("risk")
        name = bridge.get("name") or bridge.get("bridge_name")
        due_date = _serialize_date(bridge.get("next_inspection_due") or bridge.get("next_inspection_date"))

        self._interventions.append(
            {
                "bridge_id": bridge_id,
                "priority": priority,
                "risk": risk_level,
                "action": action,
                "assigned_to": assigned_to,
                "response_steps": response_time,
                "day": day,
                "t": t,
            }
        )

        status = self._statuses.get(
            bridge_id,
            BridgeStatusEntry(
                bridge_id=bridge_id,
                name=name if isinstance(name, str) else None,
                priority=str(priority) if priority else None,
                risk=str(risk_level) if risk_level else None,
                status="work_order",
                work_order_status="dispatched",
                action=action,
                due_date=due_date,
                days_overdue=None,
                lng=lng,
                lat=lat,
                last_update={},
            ),
        )

        status.priority = str(priority) if priority else status.priority
        status.risk = str(risk_level) if risk_level else status.risk
        status.status = "work_order"
        status.work_order_status = "dispatched"
        status.action = action or status.action
        status.lng = status.lng or lng
        status.lat = status.lat or lat
        status.due_date = status.due_date or due_date
        status.last_update = {"day": day, "t": t, "step": step}
        self._statuses[bridge_id] = status

    def record_work_order_status(
        self,
        task: Mapping[str, Any],
        *,
        day: int,
        t: float,
        step: int,
    ) -> None:
        """Track work orders acknowledged or in progress by crews."""

        bridge: BridgeRecord = task.get("bridge", {})  # type: ignore[assignment]
        bridge_id = _extract_bridge_id(bridge)
        priority = task.get("priority")
        action = task.get("action")
        lng, lat = _extract_coords(bridge)
        name = bridge.get("name") or bridge.get("bridge_name")

        status = self._statuses.get(
            bridge_id,
            BridgeStatusEntry(
                bridge_id=bridge_id,
                name=name if isinstance(name, str) else None,
                priority=str(priority) if priority else None,
                risk=str(task.get("risk")) if task.get("risk") else None,
                status="work_order",
                work_order_status="in_progress",
                action=action,
                due_date=None,
                days_overdue=None,
                lng=lng,
                lat=lat,
                last_update={},
            ),
        )

        status.status = "work_order"
        status.work_order_status = "in_progress"
        status.action = action or status.action
        status.priority = str(priority) if priority else status.priority
        status.lng = status.lng or lng
        status.lat = status.lat or lat
        status.last_update = {"day": day, "t": t, "step": step}
        self._statuses[bridge_id] = status

        if str(priority).lower() == "critical" and bridge_id not in self._mitigated_bridges:
            self._mitigated_bridges.add(bridge_id)

    def get_metric_tuples(self, current_step: int) -> list[tuple[str, float, int]]:
        """Return metrics ready for storage."""

        metrics: list[tuple[str, float, int]] = []
        if self._backlog_history:
            metrics.append(("bridge/current_backlog", float(self._backlog_history[-1][1]), current_step))
            initial = self._backlog_history[0][1]
            reduction = initial - self._backlog_history[-1][1]
            metrics.append(("bridge/backlog_reduction", float(reduction), current_step))

        if self._response_times:
            metrics.append(("bridge/avg_response_steps", float(mean(self._response_times)), current_step))

        critical_open = sum(
            1
            for status in self._statuses.values()
            if (status.priority or "").lower() == "critical"
            and status.status in {"inspection_due", "triaged", "work_order"}
        )
        metrics.append(("bridge/critical_open_work_orders", float(critical_open), current_step))
        metrics.append(("bridge/risk_mitigated", float(len(self._mitigated_bridges)), current_step))
        return metrics

    def export_state(self) -> dict[str, Any]:
        """Provide JSON-serializable state for artifacts and visualization."""

        return {
            "inspections": self._inspections,
            "interventions": self._interventions,
            "work_orders": [status.__dict__ for status in self._statuses.values()],
        }


# Global singleton used by agents and the simulator
bridge_monitor = BridgeMaintenanceMonitor()

__all__ = ["bridge_monitor", "BridgeMaintenanceMonitor", "BridgeStatusEntry"]
