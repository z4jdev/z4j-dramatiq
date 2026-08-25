"""Pending-task cancellation through the optional ``dramatiq-abort`` package.

Stock Dramatiq has no cancellation primitive. The separate
``dramatiq-abort`` package can prevent pending work or interrupt running work.
z4j deliberately uses only its pending mode because a running abort is seen as
an exception by Dramatiq's Retries middleware and can enqueue another attempt.
"""

from __future__ import annotations

from typing import Any

from z4j_core.models import CommandResult

from z4j_dramatiq._offload import (
    OffloadTimeoutError,
    indeterminate_timeout_result,
    offload,
)

_OFFLOAD_TIMEOUT = 10.0


async def cancel_task_action(
    broker: Any,
    *,
    task_id: str,
) -> CommandResult:
    """Prevent a pending message from starting.

    This does not interrupt work that is already running. Passing the adapter's
    own middleware instance also avoids accidentally signalling a different
    process-global Dramatiq broker.
    """
    middleware = getattr(broker, "middleware", None) or []
    try:
        abortable_type, abort, abort_mode = _load_abort_api()
    except ImportError:
        if not any(type(item).__name__ == "Abortable" for item in middleware):
            return CommandResult(
                status="failed",
                error=(
                    "cancel_task requires the dramatiq-abort Abortable middleware; "
                    "stock Dramatiq has no built-in cancel primitive"
                ),
            )
        return CommandResult(
            status="failed",
            error=(
                "cancel_task requires the `dramatiq-abort` package. Install "
                "z4j-dramatiq[abort] and add its Abortable middleware."
            ),
        )

    abortable = next(
        (item for item in middleware if isinstance(item, abortable_type)),
        None,
    )
    if abortable is None:
        return CommandResult(
            status="failed",
            error=(
                "cancel_task requires the dramatiq-abort Abortable middleware; "
                "stock Dramatiq has no built-in cancel primitive"
            ),
        )

    try:
        await offload(
            abort,
            task_id,
            middleware=abortable,
            mode=abort_mode.CANCEL,
            timeout=_OFFLOAD_TIMEOUT,
        )
    except OffloadTimeoutError:
        return indeterminate_timeout_result(
            "cancel",
            _OFFLOAD_TIMEOUT,
            hint="the pending-task cancellation may still have landed",
        )
    except Exception as exc:
        return CommandResult(status="failed", error=f"cancel failed: {exc}")

    return CommandResult(
        status="success",
        result={
            "task_id": task_id,
            "cancelled": True,
            "pending_only": True,
        },
    )


def _load_abort_api() -> tuple[type[Any], Any, Any]:
    """Load the optional API lazily so the base package stays importable."""
    from dramatiq_abort import Abortable, abort  # type: ignore[import-not-found]
    from dramatiq_abort.middleware import AbortMode  # type: ignore[import-not-found]

    return Abortable, abort, AbortMode


__all__ = ["cancel_task_action"]
