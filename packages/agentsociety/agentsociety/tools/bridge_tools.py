from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Iterable, MutableMapping, Sequence
import csv
import json


BridgeRecord = MutableMapping[str, object]


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value).date()
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%Y%m%d"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None


def load_clean_bridge_records(directory: str | Path = "data/bridge_inventory/processed") -> list[BridgeRecord]:
    """Load cleaned bridge records from CSV or JSON files.

    The loader scans the provided directory for the newest JSON or CSV file and
    returns a list of dictionaries. It is defensive: if no files are found or
    parsing fails, it returns an empty list instead of raising.
    """

    base_path = Path(directory)
    if not base_path.exists():
        return []

    candidate_files: list[Path] = []
    candidate_files.extend(sorted(base_path.glob("*.json")))
    candidate_files.extend(sorted(base_path.glob("*.csv")))
    if not candidate_files:
        return []

    file_path = max(candidate_files, key=lambda p: p.stat().st_mtime)
    try:
        if file_path.suffix.lower() == ".json":
            return json.loads(file_path.read_text())  # type: ignore[arg-type]
        records: list[BridgeRecord] = []
        with file_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                records.append(dict(row))
        return records
    except Exception:
        return []


def filter_bridges_by_condition(
    records: Iterable[BridgeRecord],
    max_condition_score: int | float = 4,
    risk_levels: Sequence[str] | None = None,
) -> list[BridgeRecord]:
    """Return bridges that match risk/condition thresholds.

    Condition ratings in the National Bridge Inventory typically use a 0-9
    scale; lower scores indicate worse conditions. This helper keeps bridges
    that are at or below ``max_condition_score`` and optionally match a set of
    risk level labels (e.g., HIGH, MEDIUM).
    """

    normalized_risks = {level.lower() for level in (risk_levels or [])}
    filtered: list[BridgeRecord] = []
    for record in records:
        condition_raw = record.get("condition_rating") or record.get("condition")
        try:
            condition_score = float(condition_raw) if condition_raw is not None else None
        except (TypeError, ValueError):
            condition_score = None

        risk_raw = record.get("risk_level") or record.get("risk")
        risk_label = str(risk_raw).lower() if risk_raw is not None else None

        if condition_score is None:
            continue
        if condition_score > float(max_condition_score):
            continue
        if normalized_risks and risk_label not in normalized_risks:
            continue
        filtered.append(record)
    return filtered


def schedule_overdue_inspections(
    records: Iterable[BridgeRecord],
    reference_date: date | None = None,
    lead_time_days: int = 30,
) -> list[dict[str, object]]:
    """Identify bridges with overdue or soon-due inspections.

    The scheduler checks ``next_inspection_due`` or ``last_inspection_date``
    fields, treating missing values as overdue. Bridges are sorted by urgency
    (oldest due date first) and annotated with ``due_date`` and
    ``days_overdue`` keys.
    """

    today = reference_date or date.today()
    backlog: list[dict[str, object]] = []
    for record in records:
        due_raw = record.get("next_inspection_due") or record.get("next_inspection_date")
        last_raw = record.get("last_inspection_date")
        due_date = _parse_date(due_raw) or _parse_date(last_raw)
        if due_date is None:
            due_date = today
        days_overdue = (today - due_date).days
        if days_overdue >= -lead_time_days:
            backlog.append({
                "bridge": record,
                "due_date": due_date,
                "days_overdue": days_overdue,
            })
    backlog.sort(key=lambda entry: (entry["days_overdue"], entry["due_date"]))
    return backlog


def propose_repair_actions(
    records: Iterable[BridgeRecord],
    severe_threshold: int | float = 3,
) -> list[dict[str, object]]:
    """Generate repair proposals based on condition severity.

    Each returned proposal contains the bridge record, a priority label, and a
    recommended action string.
    """

    proposals: list[dict[str, object]] = []
    for record in records:
        condition_raw = record.get("condition_rating") or record.get("condition")
        try:
            condition_score = float(condition_raw) if condition_raw is not None else None
        except (TypeError, ValueError):
            condition_score = None
        if condition_score is None:
            continue

        priority = "critical" if condition_score <= severe_threshold else "routine"
        risk_context = record.get("risk_level") or record.get("risk") or "unknown"
        action = "Stabilize, post warning signage, and initiate emergency repair crew dispatch" if priority == "critical" else "Schedule preventive maintenance and patch identified defects"

        proposals.append({
            "bridge": record,
            "priority": priority,
            "risk": risk_context,
            "recommended_action": action,
        })
    return proposals


__all__ = [
    "BridgeRecord",
    "filter_bridges_by_condition",
    "load_clean_bridge_records",
    "propose_repair_actions",
    "schedule_overdue_inspections",
]
