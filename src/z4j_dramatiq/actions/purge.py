"""``purge_queue`` action - empty a Dramatiq queue.

Same H13/M-7 guard as the Celery + RQ adapters: a keyed
``HMAC(project_secret, "purge|queue|depth")`` confirm-token gate (see
``z4j_core.purge_token``; keying stops a depth-observer forging or
refreshing a token, with a grace window that still accepts the pre-1.7
unkeyed token and warns), a depth threshold, and a ``force=True`` bypass.

Dramatiq exposes per-broker purge primitives:

- Redis broker: ``broker.client.delete(queue_key)``
- RabbitMQ broker: ``broker.connection.channel().queue_purge(queue_name)``

We dispatch to whichever the user's broker supports via the
broker's own ``flush(queue_name)`` method when present (Dramatiq
1.16+ exposes this on every built-in broker).
"""

from __future__ import annotations

import logging
import os
from typing import Any

from z4j_core.models import CommandResult
from z4j_core.purge_token import (
    accept_legacy_from_env,
    verify_purge_confirm_token,
)
from z4j_core.transport.hmac import decode_agent_hmac_secret

logger = logging.getLogger("z4j.adapter.dramatiq.actions.purge")

_DEFAULT_THRESHOLD = 10_000


def _resolve_agent_secret() -> bytes | None:
    """Raw per-project secret for keying the confirm token, or None.

    Reads + decodes ``Z4J_HMAC_SECRET`` the same way frame signing does;
    None (absent/undecodable) leaves only the legacy unkeyed token
    verifiable during the grace window.
    """
    raw = os.environ.get("Z4J_HMAC_SECRET")
    if not raw:
        return None
    try:
        return decode_agent_hmac_secret(raw)
    except ValueError:
        return None


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
        accepted, used_legacy = verify_purge_confirm_token(
            provided=confirm_token or "",
            queue_name=queue_name,
            queue_depth=depth,
            secret=_resolve_agent_secret(),
            accept_legacy=accept_legacy_from_env(),
        )
        if not accepted:
            return CommandResult(
                status="failed",
                error=(
                    "purge confirm_token missing or stale (queue depth "
                    "may have changed); re-issue from the dashboard"
                ),
            )
        if used_legacy:
            logger.warning(
                "z4j purge_queue: accepted a LEGACY unkeyed confirm_token "
                "for queue %r -- the issuer is pre-1.7. Upgrade the brain "
                "so it sends a keyed HMAC token; legacy acceptance is "
                "removed in a future release.",
                queue_name,
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
    except Exception as exc:
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
        except Exception:  # noqa: S110  best-effort queue depth
            pass
    fn = getattr(broker, "queue_count", None) or getattr(broker, "depth", None)
    if callable(fn):
        try:
            return int(fn(queue_name) or 0)
        except Exception:
            return 0
    return 0


def _threshold() -> int:
    raw = os.environ.get("Z4J_PURGE_THRESHOLD")
    if not raw:
        return _DEFAULT_THRESHOLD
    try:
        return max(0, int(raw))
    except ValueError:
        return _DEFAULT_THRESHOLD


__all__ = ["purge_queue_action"]
