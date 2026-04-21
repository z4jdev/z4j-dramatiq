"""Translate Dramatiq Message + result/exception into z4j Event shape.

Dramatiq's ``Message`` is a flat ``namedtuple``-ish object exposing:

- ``message_id``      - opaque id assigned at enqueue
- ``actor_name``      - the registered actor (== task name)
- ``queue_name``      - destination queue
- ``args`` / ``kwargs`` - task arguments (NEVER forward unredacted)
- ``options``         - dict of Dramatiq-internal options
- ``message_timestamp`` - UTC ms epoch the message was created

Security invariants (CLAUDE.md §2.3):

- We **never** forward ``args`` / ``kwargs`` raw. The mapper drops
  them entirely; only the actor name and queue survive.
- The exception summary is bounded to 4 KiB to keep the buffer
  from churning under a multi-MB traceback.
"""

from __future__ import annotations

import os
import socket
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from z4j_core.models import Event, EventKind
from z4j_core.redaction.engine import RedactionEngine

from z4j_dramatiq.meta import TaskMeta, get_meta

DRAMATIQ_ENGINE_NAME = "dramatiq"

_MAX_EXC_SUMMARY_BYTES = 4096


def build_event(
    *,
    kind: EventKind,
    message: Any,
    redaction: RedactionEngine,
    actor: Any | None = None,
    exception: BaseException | None = None,
    extra: dict[str, Any] | None = None,
) -> Event:
    """Construct an :class:`Event` from a Dramatiq Message."""
    actor_name = _safe_str(getattr(message, "actor_name", "unknown"))
    queue_name = _safe_str(getattr(message, "queue_name", "default"))

    payload: dict[str, Any] = {
        "task_name": actor_name,
        "queue": queue_name,
        # Worker identity: Dramatiq's middleware hooks don't pass a
        # named worker id (there isn't one at the engine level -
        # workers are identified by process). We synthesize a stable
        # name from hostname+pid so the brain's Workers page has
        # rows to render. Matches the shape RQ and Celery use.
        "worker": _worker_identity(),
    }

    if exception is not None:
        exc_summary = f"{type(exception).__name__}: {exception}"
        payload["exception"] = _safe_truncate(
            _safe_str(exc_summary), _MAX_EXC_SUMMARY_BYTES,
        )

    if extra:
        payload.update(extra)

    payload = redaction.scrub(payload)

    meta: TaskMeta | None = get_meta(actor) if actor is not None else None
    if meta and meta.tags:
        payload["tags"] = list(meta.tags)
    if meta and meta.priority:
        payload["priority"] = meta.priority

    placeholder = uuid4()  # project_id, agent_id stamped on the brain
    return Event(
        id=uuid4(),
        project_id=placeholder,
        agent_id=placeholder,
        engine=DRAMATIQ_ENGINE_NAME,
        task_id=_safe_str(getattr(message, "message_id", "")),
        kind=kind,
        occurred_at=_resolve_occurred_at(message),
        data=payload,
    )


def _worker_identity() -> str:
    """Stable ``hostname@pid`` identity for the current worker process.

    Dramatiq runs N worker processes × T threads per process; each
    process has a distinct PID and therefore a distinct identity.
    Threads inside a process share the identity, which matches the
    "worker process" abstraction the brain's Workers page shows.
    """
    try:
        host = socket.gethostname()
    except Exception:  # noqa: BLE001
        host = "unknown"
    return f"{host}@{os.getpid()}"


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    try:
        return str(value)
    except Exception:  # noqa: BLE001
        return "<unstringifiable>"


def _safe_truncate(value: str, limit: int) -> str:
    if not value:
        return value
    encoded = value.encode("utf-8", errors="replace")
    if len(encoded) <= limit:
        return value
    truncated = encoded[:limit].decode("utf-8", errors="ignore")
    return truncated + "…[truncated]"


def _resolve_occurred_at(message: Any) -> datetime:
    """Prefer the Message's own timestamp; fall back to now()."""
    ts = getattr(message, "message_timestamp", None)
    if isinstance(ts, (int, float)) and ts > 0:
        try:
            return datetime.fromtimestamp(ts / 1000.0, tz=UTC)
        except Exception:  # noqa: BLE001
            pass
    return datetime.now(UTC)


__all__ = ["DRAMATIQ_ENGINE_NAME", "build_event"]
