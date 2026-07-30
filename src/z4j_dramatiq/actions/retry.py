"""``retry`` action - re-enqueue a Dramatiq Message.

Dramatiq stores every in-flight message in the broker until ack.
Once acked, the original Message body is gone. A failed task is
therefore only re-runnable BY REFERENCE via the dead-letter queue
(the DeadLetter middleware), which preserves the original
arguments; the brain cannot supply them because it stores them
redacted (H3/M7). 1.7.1: a no-override retry requeues from the DLQ
and FAILS CLOSED when no DLQ holds the message, instead of
re-running the actor with an empty payload. Operator-supplied
``override_args`` / ``override_kwargs`` are the explicit
"retry with different inputs" path and re-send the actor directly.

Audit (H-2 / pickle-in-retry): this retry path is structurally
immune to the H-2 pattern flagged in z4j-rq. Dramatiq's default
MessageEncoder is JSON (``dramatiq.encoder.JSONEncoder``), not
pickle, but more importantly we do not touch the broker-stored
body at any point - the brain MUST supply ``actor_name`` and we
re-enqueue via ``actor.send`` / ``actor.send_with_options`` using
brain-supplied arguments only. An attacker who can write to the
broker cannot trigger deserialization inside the agent through
this surface. See also ``z4j_dramatiq.events.mapper`` (args/kwargs
dropped at the boundary) and ``z4j_dramatiq.actions.dlq``
(``actor_name`` required; native path uses Dramatiq's own
``DeadLetter.resurrect`` which republishes raw bytes server-side)."""

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


async def retry_task_action(  # noqa: PLR0911  requeue-by-ref + override + fail-closed branches
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
    """Re-run a failed Dramatiq task -- two safe paths (1.7.1).

    (a) With operator-supplied ``override_args`` / ``override_kwargs``,
    re-send the actor with THOSE arguments (the operator is the
    authority on them).

    (b) With NO overrides, requeue the ORIGINAL message BY REFERENCE
    from the dead-letter queue (``DeadLetter.resurrect``, which
    republishes the raw bytes server-side and preserves the original
    arguments). Dramatiq drops a message once acked, so without a DLQ a
    no-override retry cannot recover the original arguments -- the brain
    stores them REDACTED and cannot reconstruct them (H3/M7). We
    therefore FAIL CLOSED rather than re-run the actor with an empty
    payload, which the pre-1.7.1 code did.
    """
    # NOTE: the one-sided-override hardening (RH2) is deferred to the RH1 work,
    # which removes the __z4j_actor_name__ smuggling from override_kwargs; until
    # then this channel cannot cleanly distinguish "operator sent empty kwargs"
    # from "operator sent nothing".
    has_overrides = override_args is not None or override_kwargs is not None

    if not has_overrides:
        # Requeue-by-reference via the dead-letter queue. Needs no
        # actor_name or args -- resurrect operates on the stored message
        # by id. Offloaded to a thread (sync broker I/O) under a timeout.
        from z4j_dramatiq.actions.dlq import _try_native_dlq

        try:
            dlq_result = await offload(
                _try_native_dlq,
                broker,
                task_id=task_id,
                actor_name=actor_name or "",
                timeout=_OFFLOAD_TIMEOUT,
            )
        except OffloadTimeoutError:
            # The resurrect may still land on the broker; report
            # indeterminate rather than a clean failure.
            return indeterminate_timeout_result(
                "retry",
                _OFFLOAD_TIMEOUT,
                hint="the message may still be resurrected",
            )
        if dlq_result is not None:
            return dlq_result
        return CommandResult(
            status="failed",
            error=(
                f"cannot retry {task_id!r} by reference: no dead-letter "
                "queue holds it (Dramatiq drops a message once acked, so a "
                "failed task is only recoverable with the DeadLetter "
                "middleware configured), and no operator override_args / "
                "override_kwargs were supplied. The brain stores task "
                "arguments redacted and cannot reconstruct them. Configure "
                "the DeadLetter middleware, or use 'retry with different "
                "inputs' to supply arguments explicitly."
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

    ``actor.send`` / ``actor.send_with_options`` both drive a
    synchronous broker write (redis/rabbitmq), so this must never
    execute inline on the agent's single event loop.
    """
    if queue_name and queue_name != getattr(actor, "queue_name", None):
        return actor.send_with_options(
            args=args,
            kwargs=kwargs,
            queue_name=queue_name,
        )
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
