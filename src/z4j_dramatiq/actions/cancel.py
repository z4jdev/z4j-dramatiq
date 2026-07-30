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

from z4j_dramatiq._offload import (
    OffloadTimeoutError,
    indeterminate_timeout_result,
    offload,
)

logger = logging.getLogger("z4j.adapter.dramatiq.actions.cancel")

_OFFLOAD_TIMEOUT = 10.0


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

    # B15: dramatiq's abort lives in the SEPARATE ``dramatiq-abort``
    # package -- core ``dramatiq.middleware`` has no ``Abortable`` in modern
    # dramatiq (the old import here ALWAYS ImportError'd on dramatiq>=2, so
    # cancel was advertised by the engine's structural gate yet always
    # failed). Its middleware class is named ``Abortable`` (what the gate
    # detects) and the abort is a module-level ``abort(message_id)``.
    try:
        from dramatiq_abort import abort  # type: ignore[import-not-found]
    except ImportError:
        return CommandResult(
            status="failed",
            error=(
                "cancel_task requires the `dramatiq-abort` package. Install "
                "z4j-dramatiq[abort] (or `pip install dramatiq-abort`) and add "
                "its Abortable middleware to your broker."
            ),
        )

    try:
        await offload(abort, task_id, timeout=_OFFLOAD_TIMEOUT)
    except OffloadTimeoutError:
        # The abort message may still reach the worker; report
        # indeterminate rather than a clean failure.
        return indeterminate_timeout_result(
            "cancel",
            _OFFLOAD_TIMEOUT,
            hint="the message may still be aborted",
        )
    except Exception as exc:
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
    """True iff an ``Abortable`` middleware is in the broker's chain.

    Structural (class-name) check -- matches the engine's capability gate
    and the ``dramatiq-abort`` package's middleware, whose class is named
    ``Abortable`` (core ``dramatiq.middleware`` has none in modern
    versions, so an ``isinstance`` import check would always be False).
    """
    middleware = getattr(broker, "middleware", None)
    if not middleware:
        return False
    return any(type(mw).__name__ == "Abortable" for mw in middleware)


__all__ = ["cancel_task_action"]
