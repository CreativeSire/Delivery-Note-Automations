from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select

from models import AuditEvent, LoadingTrackerEvent, db


@dataclass
class AuditTimelineItem:
    created_at: Any
    module_name: str
    event_type: str
    entity_name: str
    summary_text: str
    details: dict[str, Any]


@dataclass
class AuditTimelineSummary:
    events: list[AuditTimelineItem]
    module_counts: list[dict[str, Any]]
    total_events: int
    selected_module: str | None


def record_audit_event(
    *,
    module_name: str,
    event_type: str,
    entity_type: str,
    entity_name: str,
    summary_text: str,
    entity_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> AuditEvent:
    event = AuditEvent(
        module_name=module_name,
        event_type=event_type,
        entity_type=entity_type,
        entity_id=entity_id,
        entity_name=entity_name,
        summary_text=summary_text,
        details_json=details or {},
    )
    db.session.add(event)
    return event


def build_audit_timeline(*, limit: int = 120, module_name: str | None = None) -> AuditTimelineSummary:
    normalized_module = (module_name or "").strip()
    selected_module = normalized_module or None

    query = select(AuditEvent).order_by(AuditEvent.created_at.desc(), AuditEvent.id.desc()).limit(limit * 2)
    if selected_module and selected_module != "Loading Tracker":
        query = (
            select(AuditEvent)
            .where(AuditEvent.module_name == selected_module)
            .order_by(AuditEvent.created_at.desc(), AuditEvent.id.desc())
            .limit(limit * 2)
        )
    audit_events = list(db.session.scalars(query))

    loading_tracker_events: list[LoadingTrackerEvent] = []
    if selected_module in (None, "Loading Tracker"):
        loading_tracker_events = list(
            db.session.scalars(
                select(LoadingTrackerEvent)
                .order_by(LoadingTrackerEvent.created_at.desc(), LoadingTrackerEvent.id.desc())
                .limit(limit * 2)
            )
        )

    events: list[AuditTimelineItem] = [
        AuditTimelineItem(
            created_at=event.created_at,
            module_name=event.module_name,
            event_type=event.event_type.replace("_", " "),
            entity_name=event.entity_name,
            summary_text=event.summary_text,
            details=event.details_json or {},
        )
        for event in audit_events
    ]

    events.extend(
        AuditTimelineItem(
            created_at=event.created_at,
            module_name="Loading Tracker",
            event_type=event.event_type.replace("_", " "),
            entity_name=event.entity_name,
            summary_text=_summarize_loading_tracker_event(event),
            details=event.details_json or {},
        )
        for event in loading_tracker_events
    )

    events.sort(key=lambda item: ((item.created_at or 0), item.summary_text), reverse=True)
    events = events[:limit]

    module_counts = Counter(item.module_name for item in events)
    return AuditTimelineSummary(
        events=events,
        module_counts=[
            {"module_name": name, "count": count}
            for name, count in sorted(module_counts.items(), key=lambda item: (-item[1], item[0]))
        ],
        total_events=len(events),
        selected_module=selected_module,
    )


def _summarize_loading_tracker_event(event: LoadingTrackerEvent) -> str:
    event_name = event.event_type.replace("_", " ")
    reason = event.reason_text or ""
    if reason:
        return f"{event_name.title()} with reason '{reason}'."
    day_name = event.day.day_name if event.day is not None else ""
    if day_name:
        return f"{event_name.title()} on {day_name}."
    return f"{event_name.title()} recorded."
