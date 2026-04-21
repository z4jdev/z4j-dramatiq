"""``cancel`` action - Abortable-gated cancel for Dramatiq.

Dramatiq supports cancel ONLY when the user has the
``dramatiq.middleware.Abortable`` middleware installed. The engine
adapter checks the broker's middleware stack at startup and only
advertises ``cancel_task`` in :meth:`capabilities` when it is.

If a cancel command somehow reaches this action without Abortable
being installed (e.g. a brain that ignores the capability set),
we fail loudly rather than silently no-op - that's the only honest
behaviour for an action the engine cannot perform.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.models import CommandResult

logger = logging.getLogger("z4j.agent.dramatiq.actions.cancel")


async def cancel_task_action(
    broker: Any,
    *,
    task_id: str,
) -> CommandResult:
    """Send an abort-message to the worker that owns ``task_id``."""
    if not _broker_has_abortable(broker):
        return CommandResult(
            status="failed",
            error=(
                "cancel_task requires the Dramatiq Abortable middleware. "
                "Add it to your broker.middleware stack and restart the "
                "worker. See: "
                "https://dramatiq.io/reference.html#dramatiq.middleware.Abortable"
            ),
        )

    try:
        from dramatiq.middleware import (  # type: ignore[import-not-found]
            Abortable,
        )
    except ImportError:
        return CommandResult(
            status="failed",
            error="dramatiq.middleware.Abortable not importable",
        )

    try:
        Abortable.abort(task_id)
    except Exception as exc:  # noqa: BLE001
        return CommandResult(status="failed", error=f"cancel failed: {exc}")

    return CommandResult(
        status="success",
        result={
            "task_id": task_id,
            "soft": True,
            "note": "abort signalled; worker will honor at next checkpoint",
        },
    )


def _broker_has_abortable(broker: Any) -> bool:
    """True iff the user has ``Abortable`` in their middleware chain."""
    middleware = getattr(broker, "middleware", None)
    if not middleware:
        return False
    try:
        from dramatiq.middleware import (  # type: ignore[import-not-found]
            Abortable,
        )
    except ImportError:
        return False
    return any(isinstance(mw, Abortable) for mw in middleware)


__all__ = ["cancel_task_action"]
