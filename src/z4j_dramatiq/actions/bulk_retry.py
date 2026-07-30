"""``bulk_retry`` action for the Dramatiq engine adapter.

Dramatiq's wire model is "fire-and-forget to broker" - once an
actor.send() lands in the broker and gets consumed, the original
Message is gone. A failed message is only re-runnable BY REFERENCE
via the dead-letter queue (the DeadLetter middleware).

1.7.1 (CX-H5, security): this action re-runs each id BY REFERENCE
through the DLQ (``retry_task_action`` with no overrides ->
``DeadLetter.resurrect``). It does NOT read any executable field from
the filter. The pre-1.7.1 code read client-supplied ``actors`` /
``queues`` / ``args`` / ``kwargs`` maps and invoked the named actor
with them, which -- because the brain's filter sanitizer only stripped
a 3-key denylist -- let a project OPERATOR invoke an ARBITRARY
registered actor with attacker-chosen arguments (a confused-deputy
actor-invocation primitive). The brain now strips those keys via a
selection-only allowlist; this action ignores them as defence in
depth. An id whose message is not in a DLQ fails closed for that id.

1.7.1 (M10, reliability): a consecutive-timeout circuit breaker aborts the
batch after a short run of broker timeouts (each burns the full offload
timeout inline on the receive loop) rather than grinding through every id
while the command channel is frozen. Ported from the RQ/Celery bulk paths.

Filter shape (selection only): ``{"task_ids": ["msg-1", ...]}``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from z4j_core.models import CommandResult

from z4j_dramatiq.actions.retry import retry_task_action

logger = logging.getLogger("z4j.adapter.dramatiq.actions.bulk_retry")

_MAX_ABSOLUTE = 10_000
_YIELD_EVERY = 100


async def bulk_retry_action(
    broker: Any,
    *,
    filter: dict[str, Any] | None = None,  # noqa: A002  public bulk_retry signature
    max: int = 1000,  # noqa: A002  public bulk_retry signature
) -> CommandResult:
    """Requeue up to ``max`` failed messages BY REFERENCE; summary dict.

    Only ``filter["task_ids"]`` (the selection) is consulted. Every id
    is re-run through the dead-letter queue via ``retry_task_action``
    with no overrides; executable filter fields (actors/args/kwargs/
    queues) are IGNORED (CX-H5). An id not held by a DLQ is recorded as
    a per-id error.
    """
    filter = filter or {}  # noqa: A001  public bulk_retry signature
    effective_max = min(max, _MAX_ABSOLUTE)
    capped = max > _MAX_ABSOLUTE

    raw_ids = filter.get("task_ids") or []
    if not isinstance(raw_ids, list):
        raw_ids = []
    task_ids = [str(t) for t in raw_ids][:effective_max]

    retried = 0
    skipped = 0
    new_ids: list[str] = []
    errors: dict[str, str] = {}
    # M10: circuit breaker. Each id whose DLQ resurrect is offloaded against a
    # hung broker burns the full offload timeout (~10s) inline on the receive
    # loop; a large batch would freeze command handling and stall event acks for
    # minutes while heartbeats keep flowing and the agent still looks healthy.
    # Abort after a short run of CONSECUTIVE broker timeouts rather than grinding
    # through every id (ported from the RQ/Celery bulk paths).
    circuit_break_after = 3
    consecutive_timeouts = 0
    broker_unhealthy = False

    for i, task_id in enumerate(task_ids, start=1):
        # Requeue-by-reference only: no overrides, no client-supplied
        # actor/args/kwargs. retry_task_action resurrects the original
        # message from the DLQ (or fails closed for this id).
        result = await retry_task_action(broker, task_id=task_id)
        # An offload timeout tags result["indeterminate"] -- the broker-hang
        # signal the breaker counts. M2: only a genuine SUCCESS resets the
        # counter; a determinate failure is neutral (neither trips nor
        # resets), so an alternating timeout/failure pattern cannot starve
        # the breaker into grinding the whole batch against a hung broker.
        if result.result and result.result.get("indeterminate"):
            consecutive_timeouts += 1
        elif result.status == "success":
            consecutive_timeouts = 0
        if result.status == "success":
            retried += 1
            new_id = (result.result or {}).get("task_id")
            if new_id:
                new_ids.append(str(new_id))
        else:
            errors[task_id] = result.error or "unknown"

        if consecutive_timeouts >= circuit_break_after:
            broker_unhealthy = True
            # Everything after this id is skipped: the broker is clearly
            # hung and grinding on would freeze the receive loop.
            skipped += len(task_ids) - i
            break

        if i % _YIELD_EVERY == 0:
            await asyncio.sleep(0)

    return CommandResult(
        status="success",
        result={
            "retried": retried,
            "skipped": skipped,
            "capped": capped or len(raw_ids) > effective_max,
            "circuit_broken": broker_unhealthy,
            "new_task_ids": new_ids,
            "errors": errors,
        },
    )


__all__ = ["bulk_retry_action"]
