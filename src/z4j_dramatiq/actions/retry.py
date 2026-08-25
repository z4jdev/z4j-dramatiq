"""``retry`` action - re-enqueue a Dramatiq Message.

Dramatiq drops an acknowledged message and stock Dramatiq exposes no
recoverable dead-letter API. A retry is therefore available only when the
operator supplies both complete argument collections. The brain cannot replay
its stored copy because task arguments are redacted.

Audit (H-2 / pickle-in-retry): this retry path is structurally
immune to the H-2 pattern flagged in z4j-rq. Dramatiq's default
MessageEncoder is JSON (``dramatiq.encoder.JSONEncoder``), not
pickle, but more importantly we do not touch the broker-stored
body at any point - the brain MUST supply ``actor_name`` and we
re-enqueue via ``actor.send`` / ``actor.send_with_options`` using
brain-supplied arguments only. An attacker who can write to the
broker cannot trigger deserialization inside the agent through
this surface. See also ``z4j_dramatiq.events.mapper`` (args/kwargs
dropped at the boundary). Stock Dramatiq has no recoverable
dead-letter API, so this action never attempts a by-reference replay."""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.models import CommandResult

from z4j_dramatiq._offload import (
    OffloadTimeoutError,
    indeterminate_timeout_result,
    offload,
)

logger = logging.getLogger("z4j.adapter.dramatiq.actions.retry")

_OFFLOAD_TIMEOUT = 10.0


async def retry_task_action(  # noqa: PLR0911  validation + offload failure branches
    broker: Any,
    *,
    task_id: str,
    actor_name: str | None = None,
    queue_name: str | None = None,
    override_args: tuple[Any, ...] | None = None,
    override_kwargs: dict[str, Any] | None = None,
    eta: float | None = None,
    priority: object = None,
) -> CommandResult:
    """Re-run a task only from complete operator-supplied replacements."""
    if (override_args is None) != (override_kwargs is None):
        return CommandResult(
            status="failed",
            error=(
                "Dramatiq retry requires both complete override_args and "
                "override_kwargs; the missing half cannot be recovered from "
                "the brain's redacted task snapshot"
            ),
        )
    has_overrides = override_args is not None and override_kwargs is not None

    if not has_overrides:
        return CommandResult(
            status="failed",
            error=(
                f"cannot retry {task_id!r} by reference: stock Dramatiq has "
                "no recoverable dead-letter API, and no operator "
                "override_args / override_kwargs were supplied. The brain "
                "stores task arguments redacted; use 'retry with different "
                "inputs' and supply both complete argument collections."
            ),
        )

    # Operator-supplied overrides: re-send via the actor with those args.
    if not actor_name:
        return CommandResult(
            status="failed",
            error=(
                f"retry of {task_id!r} with overrides requires actor_name "
                "(the brain passes it from the original message snapshot)"
            ),
        )

    actor = _resolve_actor(broker, actor_name)
    if actor is None:
        return CommandResult(
            status="failed",
            error=f"actor {actor_name!r} is not registered on this broker",
        )

    if eta is not None or priority is not None:
        unsupported = [
            option for option, value in (("eta", eta), ("priority", priority)) if value is not None
        ]
        return CommandResult(
            status="failed",
            error=("z4j-dramatiq cannot portably honor retry option(s): " + ", ".join(unsupported)),
        )

    if queue_name and queue_name != getattr(actor, "queue_name", None):
        return CommandResult(
            status="failed",
            error=(
                f"actor {actor_name!r} is registered on queue "
                f"{getattr(actor, 'queue_name', None)!r}; Dramatiq cannot "
                "override an actor's queue at send time"
            ),
        )

    args = tuple(override_args) if override_args is not None else ()
    kwargs = dict(override_kwargs) if override_kwargs is not None else {}

    try:
        new_msg = await offload(
            _send_message,
            actor,
            args,
            kwargs,
            queue_name,
            timeout=_OFFLOAD_TIMEOUT,
        )
    except OffloadTimeoutError:
        # The actor re-send may still land on the broker; report
        # indeterminate rather than a clean failure.
        return indeterminate_timeout_result(
            "retry",
            _OFFLOAD_TIMEOUT,
            hint="the message may still be re-enqueued",
        )
    except Exception as exc:
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


def _send_message(
    actor: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    queue_name: str | None,
) -> Any:
    """Synchronous broker enqueue - runs off the event loop.

    ``actor.send`` drives a synchronous broker write (redis/rabbitmq), so this
    must never execute inline on the agent's single event loop.
    """
    return actor.send(*args, **kwargs)


def _resolve_actor(broker: Any, actor_name: str) -> Any | None:
    fn = getattr(broker, "get_actor", None)
    if callable(fn):
        try:
            return fn(actor_name)
        except Exception:
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
    except Exception:
        return ""


__all__ = ["retry_task_action"]
