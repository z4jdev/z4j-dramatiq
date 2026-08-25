"""Fail-closed dead-letter action for Dramatiq.

Stock Dramatiq exposes no recover-by-id or public resurrection API for
exhausted messages. It does not provide the ``DeadLetter``,
``get_dead_letter``, or ``resurrect`` interfaces earlier versions of this
adapter assumed.
"""

from __future__ import annotations

from typing import Any

from z4j_core.models import CommandResult


async def requeue_dead_letter_action(
    broker: Any,
    *,
    task_id: str,
    actor_name: str | None = None,
    queue_name: str | None = None,
    override_args: tuple[Any, ...] | None = None,
    override_kwargs: dict[str, Any] | None = None,
) -> CommandResult:
    """Refuse an action stock Dramatiq cannot implement."""
    del broker, actor_name, queue_name, override_args, override_kwargs
    return CommandResult(
        status="failed",
        error=(
            f"cannot requeue {task_id!r}: stock Dramatiq exposes no recoverable dead-letter API"
        ),
    )


__all__ = ["requeue_dead_letter_action"]
