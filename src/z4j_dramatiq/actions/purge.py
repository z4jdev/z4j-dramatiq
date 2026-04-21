"""``purge_queue`` action - empty a Dramatiq queue.

Same H13-style guard as the Celery + RQ adapters: confirm-token
gate, depth threshold, ``force=True`` bypass.

Dramatiq exposes per-broker purge primitives:

- Redis broker: ``broker.client.delete(queue_key)``
- RabbitMQ broker: ``broker.connection.channel().queue_purge(queue_name)``

We dispatch to whichever the user's broker supports via the
broker's own ``flush(queue_name)`` method when present (Dramatiq
1.16+ exposes this on every built-in broker).
"""

from __future__ import annotations

import hashlib
import logging
import os
from typing import Any

from z4j_core.models import CommandResult

logger = logging.getLogger("z4j.agent.dramatiq.actions.purge")

_DEFAULT_THRESHOLD = 10_000


async def purge_queue_action(
    broker: Any,
    *,
    queue_name: str,
    confirm_token: str | None = None,
    force: bool = False,
) -> CommandResult:
    """Empty ``queue_name`` after token + threshold checks."""
    depth = _depth(broker, queue_name)

    if not force:
        threshold = _threshold()
        if depth > threshold:
            return CommandResult(
                status="failed",
                error=(
                    f"refusing to purge {queue_name!r}: depth {depth} "
                    f"exceeds Z4J_PURGE_THRESHOLD={threshold}. Re-issue "
                    "with force=true if this is intentional."
                ),
            )
        expected = _derive_token(queue_name, depth)
        if not confirm_token or confirm_token != expected:
            return CommandResult(
                status="failed",
                error=(
                    "purge confirm_token missing or stale (queue depth "
                    "may have changed); re-issue from the dashboard"
                ),
            )

    purge = getattr(broker, "flush", None) or getattr(broker, "purge", None)
    if not callable(purge):
        return CommandResult(
            status="failed",
            error=(
                "broker has no flush/purge method; upgrade dramatiq to "
                ">=1.16 or implement broker.flush(queue_name) on your "
                "custom broker"
            ),
        )

    try:
        purge(queue_name)
    except Exception as exc:  # noqa: BLE001
        return CommandResult(status="failed", error=f"purge failed: {exc}")
    return CommandResult(
        status="success",
        result={"queue": queue_name, "purged": depth},
    )


def _depth(broker: Any, queue_name: str) -> int:
    """Best-effort current queue depth - broker-shape dependent."""
    fn = getattr(broker, "get_queue_message_counts", None)
    if callable(fn):
        try:
            counts = fn(queue_name)
            # Dramatiq returns (queued, delayed, dead) tuple.
            if isinstance(counts, (tuple, list)) and counts:
                return int(counts[0] or 0)
        except Exception:  # noqa: BLE001
            pass
    fn = getattr(broker, "queue_count", None) or getattr(broker, "depth", None)
    if callable(fn):
        try:
            return int(fn(queue_name) or 0)
        except Exception:  # noqa: BLE001
            return 0
    return 0


def _derive_token(queue_name: str, depth: int) -> str:
    payload = f"{queue_name}|{depth}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _threshold() -> int:
    raw = os.environ.get("Z4J_PURGE_THRESHOLD")
    if not raw:
        return _DEFAULT_THRESHOLD
    try:
        return max(0, int(raw))
    except ValueError:
        return _DEFAULT_THRESHOLD


__all__ = ["purge_queue_action"]
