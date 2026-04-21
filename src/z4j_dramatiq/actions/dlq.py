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

from z4j_dramatiq.actions.retry import retry_task_action

logger = logging.getLogger("z4j.agent.dramatiq.actions.dlq")


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

    ``actor_name`` is required (same invariant as
    :func:`retry_task_action`) - the brain passes it from its
    Message snapshot.
    """
    if not actor_name:
        return CommandResult(
            status="failed",
            error=(
                f"requeue_dead_letter of {task_id!r} requires "
                "actor_name (the brain should pass the original "
                "message's actor identity)"
            ),
        )

    dlq_result = _try_native_dlq(broker, task_id=task_id,
                                 actor_name=actor_name)
    if dlq_result is not None:
        return dlq_result

    # Fallback: generic retry via the actor. Always marks the
    # source so the audit log distinguishes this from a clean DLQ
    # resurrection.
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
    broker: Any, *, task_id: str, actor_name: str,
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
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "z4j dramatiq: get_dead_letter raised - falling back: %s", exc,
        )
        return None
    if not dead_letters:
        return None

    # DeadLetter.resurrect(msg) is the blessed recovery path if the
    # middleware instance is reachable. Otherwise fall back.
    middleware = getattr(broker, "middleware", None) or []
    for mw in middleware:
        if type(mw).__name__ == "DeadLetter":
            resurrect = getattr(mw, "resurrect", None)
            if callable(resurrect):
                try:
                    resurrect(task_id)
                    return CommandResult(
                        status="success",
                        result={
                            "task_id": task_id,
                            "actor_name": actor_name,
                            "source": "dlq",
                        },
                    )
                except Exception as exc:  # noqa: BLE001
                    return CommandResult(
                        status="failed",
                        error=f"DeadLetter.resurrect failed: {exc}",
                    )
    return None


__all__ = ["requeue_dead_letter_action"]
