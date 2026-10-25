"""Routed output builders for work, scheduler, and direct-topic delivery."""

from __future__ import annotations

from typing import Any

from materializer_control_plane.models import MaterializationWorkEvent
from materializer_control_plane.storage import materialization_routing_key


def build_direct_work_output(work_event: MaterializationWorkEvent) -> dict:
    return {
        "route": "work",
        "key": materialization_routing_key(work_event.materialization_id),
        "value": work_event.to_dict(),
        "headers": {},
    }


def build_scheduled_work_output(
    work_event: MaterializationWorkEvent,
    *,
    request_id: str,
    deliver_to: str,
    deliver_at: str,
) -> dict:
    return {
        "route": "scheduler",
        "key": materialization_routing_key(work_event.materialization_id),
        "value": work_event.to_dict(),
        "headers": {
            "x-scheduler-action": b"REPLACE",
            "x-scheduler-request-id": request_id.encode("utf-8"),
            "x-scheduler-deliver-to": deliver_to.encode("utf-8"),
            "x-scheduler-deliver-at": deliver_at.encode("utf-8"),
        },
    }


def build_scheduled_topic_output(
    *,
    topic: str,
    key: str,
    value: dict[str, Any],
    request_id: str,
    deliver_at: str,
) -> dict:
    return {
        "route": "scheduler",
        "key": key,
        "value": value,
        "headers": {
            "x-scheduler-action": b"REPLACE",
            "x-scheduler-request-id": request_id.encode("utf-8"),
            "x-scheduler-deliver-to": topic.encode("utf-8"),
            "x-scheduler-deliver-at": deliver_at.encode("utf-8"),
        },
    }


def build_direct_topic_output(
    *,
    topic: str,
    key: str,
    value: dict[str, Any],
) -> dict:
    return {
        "route": "topic",
        "key": key,
        "value": value,
        "topic": topic,
        "headers": {},
    }


__all__ = [
    "build_direct_topic_output",
    "build_direct_work_output",
    "build_scheduled_topic_output",
    "build_scheduled_work_output",
]