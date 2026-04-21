"""Dramatiq adapter actions.

Day-1 surface (per `docs/MULTI_ENGINE_PLAN.md` §5):

- :func:`retry_task_action` - re-send the original Message.
- :func:`cancel_task_action` - Abortable-gated; the engine adapter
  promotes ``cancel_task`` into the capability set only when the
  user has the ``Abortable`` middleware installed.
- :func:`purge_queue_action` - empty a queue with confirm-token guard.

Deferred to v1.1 (also §5):

- bulk_retry, requeue_dead_letter, rate_limit, restart_worker.
"""

from __future__ import annotations

from z4j_dramatiq.actions.bulk_retry import bulk_retry_action
from z4j_dramatiq.actions.cancel import cancel_task_action
from z4j_dramatiq.actions.dlq import requeue_dead_letter_action
from z4j_dramatiq.actions.purge import purge_queue_action
from z4j_dramatiq.actions.retry import retry_task_action

__all__ = [
    "bulk_retry_action",
    "cancel_task_action",
    "purge_queue_action",
    "requeue_dead_letter_action",
    "retry_task_action",
]
