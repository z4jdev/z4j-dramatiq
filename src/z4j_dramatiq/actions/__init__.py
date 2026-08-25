"""Dramatiq adapter actions.

The base portable surface is submit, retry with complete operator replacement
inputs, and guarded queue purge. Pending-task cancel is promoted only with the
external ``dramatiq-abort`` Abortable middleware. Bulk-retry and dead-letter
helpers remain importable for compatibility but fail closed because stock
Dramatiq has no recoverable dead-letter primitive.
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
