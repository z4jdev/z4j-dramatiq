"""``requeue_dead_letter`` action for the Dramatiq engine adapter.

Dramatiq's dead-letter story depends on the user's middleware stack:

- If the user has ``dramatiq.middleware.DeadLetter`` (or the
  built-in ``Retries`` middleware configured with
  ``max_retries + dead_letter_*``) the failed message lands on a
  dead-letter queue. We resurrect it by re-sending through the
  broker's registered actor.

- Without any DLQ middleware, failed messages simply disappear
  from the broker once ack'd. In that case the best we can do is
  fall back to generic retry using the caller-supplied actor name
  (the brain passes it from its task snapshot).

Either path preserves task identity and reports ``source`` so the
audit log records "dlq" vs "dlq_fallback" distinctly.
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
from z4j_dramatiq.actions.retry import retry_task_action

logger = logging.getLogger("z4j.adapter.dramatiq.actions.dlq")

_OFFLOAD_TIMEOUT = 10.0


def is_dead_letter_middleware(mw: Any) -> bool:
    """True iff ``mw`` is a DeadLetter middleware -- or a subclass / proxy of one
    -- exposing the callable ``resurrect`` the recovery path drives.

    dramatiq:473: an EXACT leaf-class-name match (``type(mw).__name__ ==
    "DeadLetter"``) rejects a compatible SUBCLASS or proxy that fully implements
    the interface. Match the name across the whole MRO so a subclass of
    ``DeadLetter`` still qualifies, while still requiring the callable
    ``resurrect`` so an unrelated middleware cannot slip through.
    """
    if not callable(getattr(mw, "resurrect", None)):
        return False
    return any(base.__name__ == "DeadLetter" for base in type(mw).__mro__)


async def requeue_dead_letter_action(
    broker: Any,
    *,
    task_id: str,
    actor_name: str | None = None,
    queue_name: str | None = None,
    override_args: tuple[Any, ...] | None = None,
    override_kwargs: dict[str, Any] | None = None,
) -> CommandResult:
    """Resurrect a failed Dramatiq message.

    1.7.1 (CX-M19): the native DLQ resurrect path operates on the
    message BY ID (``DeadLetter.resurrect(task_id)`` republishes the raw
    bytes server-side) and needs no ``actor_name``, so we try it FIRST
    and unconditionally. Only the explicit re-send FALLBACK (used when no
    DLQ holds the message) needs ``actor_name`` plus operator overrides;
    without them we fail closed. This also decouples the fix from the
    bare dispatcher version -- a DLQ-backed requeue no longer requires
    the dispatcher to have encoded the actor identity.
    """
    # ``_try_native_dlq`` runs synchronous broker I/O
    # (broker.get_dead_letter + DeadLetter.resurrect). Offload it to a
    # thread under a timeout so a broker slowdown/failover can't freeze
    # the agent's event loop (heartbeat / send loop / WS ping-pong).
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
            "requeue_dead_letter",
            _OFFLOAD_TIMEOUT,
            hint="the message may still be resurrected",
        )
    if dlq_result is not None:
        return dlq_result

    # No native DLQ holds the message. The only remaining way to re-run
    # it is an explicit re-send, which needs a brain-supplied actor_name
    # AND operator-supplied arguments (the brain stores args redacted and
    # cannot reconstruct them). Without both, fail closed.
    has_overrides = override_args is not None or override_kwargs is not None
    if not actor_name or not has_overrides:
        return CommandResult(
            status="failed",
            error=(
                f"requeue_dead_letter of {task_id!r}: no dead-letter queue "
                "holds this message (configure the DeadLetter middleware to "
                "requeue by reference), and an explicit re-send needs both a "
                "brain-supplied actor_name and operator override_args / "
                "override_kwargs (the brain stores task arguments redacted)."
            ),
        )
    result = await retry_task_action(
        broker,
        task_id=task_id,
        actor_name=actor_name,
        queue_name=queue_name,
        override_args=override_args,
        override_kwargs=override_kwargs,
    )
    if result.status == "success" and result.result:
        enriched = dict(result.result)
        enriched["source"] = "dlq_fallback"
        return CommandResult(status="success", result=enriched)
    return result


def _try_native_dlq(
    broker: Any,
    *,
    task_id: str,
    actor_name: str,
) -> CommandResult | None:
    """Best-effort native DLQ-aware path.

    Dramatiq doesn't ship a universal DLQ API - the DeadLetter
    middleware stashes messages in broker-specific ways. This
    helper attempts the most common shape (broker.get_dead_letter)
    and returns None when the broker doesn't expose one, letting
    the caller fall back.
    """
    fetcher = getattr(broker, "get_dead_letter", None)
    if not callable(fetcher):
        return None
    try:
        dead_letters = fetcher(task_id)
    except Exception as exc:
        logger.debug(
            "z4j dramatiq: get_dead_letter raised - falling back: %s",
            exc,
        )
        return None
    if not dead_letters:
        return None

    # DeadLetter.resurrect(msg) is the blessed recovery path if the
    # middleware instance is reachable. Otherwise fall back.
    middleware = getattr(broker, "middleware", None) or []
    for mw in middleware:
        # dramatiq:473: accept a DeadLetter subclass / proxy, not only the exact
        # leaf class (must match the _has_dlq capability gate).
        if is_dead_letter_middleware(mw):
            resurrect = getattr(mw, "resurrect", None)
            if callable(resurrect):
                try:
                    ok = resurrect(task_id)
                except Exception as exc:
                    return CommandResult(
                        status="failed",
                        error=f"DeadLetter.resurrect failed: {exc}",
                    )
                # M3/RL1: the DeadLetter interface has no defined return
                # contract, so disambiguate before claiming success:
                #  - None (no signal): re-check get_dead_letter -- only a
                #    confirmed-gone id is a success; if it is still there (or we
                #    cannot confirm) the resurrect was a no-op (e.g. the message
                #    disappeared between lookup and resurrect), so fall through.
                #  - explicit falsey (False/0/""): not resurrected -> fall
                #    through to the actor-based fallback (which fails closed).
                #  - truthy: resurrected.
                # dramatiq:155 (deferred): "gone from the DLQ" is the best signal
                # this interface exposes, but it cannot distinguish a real
                # resurrect (real dramatiq DeadLetter.resurrect returns None and
                # moves the message) from an expiry / concurrent delete. Flipping
                # the normal None+gone success to INDETERMINATE would mark every
                # legitimate requeue as failed. A sound fix needs a POSITIVE
                # live-queue re-enqueue confirmation, which the broker does not
                # provide; tracked for a broker-specific follow-up.
                if ok is None:
                    try:
                        still_there = fetcher(task_id)
                    except Exception:
                        still_there = True
                    if still_there:
                        break
                elif not ok:
                    break
                return CommandResult(
                    status="success",
                    result={
                        "task_id": task_id,
                        "actor_name": actor_name,
                        "source": "dlq",
                    },
                )
    return None


__all__ = ["requeue_dead_letter_action"]
