"""``retry`` action - re-enqueue a Dramatiq Message.

Dramatiq stores every in-flight message in the broker until ack.
Once acked, the original Message body is gone - but the brain has
the actor name + queue + (redacted) args reference, so we can
reconstruct + re-enqueue from the snapshot the brain holds.

The adapter accepts the snapshot via the ``override_args`` /
``override_kwargs`` parameters when the original is no longer
available; otherwise it re-uses whatever the broker still has.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.models import CommandResult

logger = logging.getLogger("z4j.agent.dramatiq.actions.retry")


async def retry_task_action(
    broker: Any,
    *,
    task_id: str,
    actor_name: str | None = None,
    queue_name: str | None = None,
    override_args: tuple[Any, ...] | None = None,
    override_kwargs: dict[str, Any] | None = None,
    eta: float | None = None,  # noqa: ARG001 (Dramatiq has its own delay arg)
    priority: object = None,  # noqa: ARG001 (per-actor priority only)
) -> CommandResult:
    """Re-send the actor's message.

    Either ``actor_name`` is provided (preferred - brain looks it
    up from the original Message) or we cannot reconstruct the
    target. Without an actor name we fail cleanly.
    """
    if not actor_name:
        return CommandResult(
            status="failed",
            error=(
                f"retry of {task_id!r} requires actor_name (the brain "
                "should pass the snapshot from the original Message)"
            ),
        )

    actor = _resolve_actor(broker, actor_name)
    if actor is None:
        return CommandResult(
            status="failed",
            error=f"actor {actor_name!r} is not registered on this broker",
        )

    args = tuple(override_args) if override_args is not None else ()
    kwargs = dict(override_kwargs) if override_kwargs is not None else {}

    try:
        if queue_name and queue_name != getattr(actor, "queue_name", None):
            new_msg = actor.send_with_options(
                args=args, kwargs=kwargs, queue_name=queue_name,
            )
        else:
            new_msg = actor.send(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001
        return CommandResult(status="failed", error=f"retry failed: {exc}")

    return CommandResult(
        status="success",
        result={
            "task_id": _safe_str(getattr(new_msg, "message_id", "")),
            "queue": _safe_str(
                getattr(new_msg, "queue_name", queue_name) or "default",
            ),
            "previous_task_id": task_id,
        },
    )


def _resolve_actor(broker: Any, actor_name: str) -> Any | None:
    fn = getattr(broker, "get_actor", None)
    if callable(fn):
        try:
            return fn(actor_name)
        except Exception:  # noqa: BLE001
            return None
    actors = getattr(broker, "actors", None)
    if actors:
        return actors.get(actor_name)
    return None


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    try:
        return str(value)
    except Exception:  # noqa: BLE001
        return ""


__all__ = ["retry_task_action"]
