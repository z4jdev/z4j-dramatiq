"""``bulk_retry`` action for the Dramatiq engine adapter.

Dramatiq's wire model is "fire-and-forget to broker" - once an
actor.send() lands in the broker and gets consumed, the original
Message is gone. The brain keeps a snapshot of every message it
has observed (on task.received), so the bulk-retry path reads
``(actor_name, queue_name)`` per task from the filter and re-sends
via the broker's registered actor.

Filter shape:

    {
      "task_ids": ["msg-1", "msg-2", ...],
      "actors":   {"msg-1": "myapp.send_invoice", ...},
      "queues":   {"msg-1": "emails", ...},       # optional
      "args":     {"msg-1": [...], ...},          # optional
      "kwargs":   {"msg-1": {...}, ...},          # optional
    }

The brain is responsible for populating ``actors`` from its own
task snapshot. If a task_id has no actor mapping we skip it and
return the skip count - a bulk retry won't silently misroute.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from z4j_core.models import CommandResult

from z4j_dramatiq.actions.retry import retry_task_action

logger = logging.getLogger("z4j.agent.dramatiq.actions.bulk_retry")

_MAX_ABSOLUTE = 10_000
_YIELD_EVERY = 100


async def bulk_retry_action(
    broker: Any,
    *,
    filter: dict[str, Any] | None = None,
    max: int = 1000,
) -> CommandResult:
    """Re-send up to ``max`` actor messages; returns a summary dict."""
    filter = filter or {}
    effective_max = min(max, _MAX_ABSOLUTE)
    capped = max > _MAX_ABSOLUTE

    raw_ids = filter.get("task_ids") or []
    if not isinstance(raw_ids, list):
        raw_ids = []
    task_ids = [str(t) for t in raw_ids][:effective_max]

    actors_map = filter.get("actors") or {}
    queues_map = filter.get("queues") or {}
    args_map = filter.get("args") or {}
    kwargs_map = filter.get("kwargs") or {}

    retried = 0
    skipped = 0
    new_ids: list[str] = []
    errors: dict[str, str] = {}

    for i, task_id in enumerate(task_ids, start=1):
        actor_name = actors_map.get(task_id)
        if not actor_name:
            skipped += 1
            continue
        queue_name = queues_map.get(task_id)
        args = args_map.get(task_id)
        kwargs = kwargs_map.get(task_id)

        result = await retry_task_action(
            broker,
            task_id=task_id,
            actor_name=str(actor_name),
            queue_name=str(queue_name) if queue_name else None,
            override_args=tuple(args) if isinstance(args, list) else None,
            override_kwargs=dict(kwargs) if isinstance(kwargs, dict) else None,
        )
        if result.status == "success":
            retried += 1
            new_id = (result.result or {}).get("task_id")
            if new_id:
                new_ids.append(str(new_id))
        else:
            errors[task_id] = result.error or "unknown"

        if i % _YIELD_EVERY == 0:
            await asyncio.sleep(0)

    return CommandResult(
        status="success",
        result={
            "retried": retried,
            "skipped": skipped,
            "capped": capped or len(raw_ids) > effective_max,
            "new_task_ids": new_ids,
            "errors": errors,
        },
    )


__all__ = ["bulk_retry_action"]
