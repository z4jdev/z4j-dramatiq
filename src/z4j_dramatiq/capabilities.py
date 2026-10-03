"""Capability tokens advertised by the Dramatiq engine adapter.

Dramatiq's portable action surface is task submission, retry with complete
operator-supplied replacement inputs, and guarded queue purge.

See `docs/MULTI_ENGINE_PLAN.md` §5 for the per-engine matrix.

The separate ``dramatiq-abort`` package can revoke pending work. z4j advertises
cancel only when its ``Abortable`` middleware is present and deliberately uses
pending-only mode: running abort interacts unsafely with Dramatiq retries.
Stock Dramatiq has no recoverable dead-letter API, but both built-in brokers
keep dead letters in a ``<queue>.XQ`` store that can be read, so
``list_dead_letters`` is promoted per broker (see
:data:`DEAD_LETTER_LISTING_CAPABILITIES`).
"""

from __future__ import annotations

# Lower-bound - what every Dramatiq install gets, with no
# middleware contortions required from the user.
DEFAULT_CAPABILITIES: frozenset[str] = frozenset(
    {
        "submit_task",
        "retry_task",
        "purge_queue",
    },
)

# Promoted only when the external Abortable middleware is present. The action
# uses dramatiq-abort's pending-only mode and does not claim to interrupt work
# that is already running.
ABORTABLE_CAPABILITIES: frozenset[str] = DEFAULT_CAPABILITIES | {"cancel_task"}

# Promoted when the broker's dead-letter store can be read without consuming
# it (``z4j_dramatiq.actions.dlq.LISTABLE_BROKER_KINDS``). What the page
# carries depends on the broker:
#
# - Redis: full entries (id, actor, dead-letter time, attempts, redacted
#   traceback excerpt) from ``<ns>:<queue>.XQ`` and ``<ns>:<queue>.XQ.msgs``.
#   Bodies are decoded as JSON only; a ``PickleEncoder`` deployment gets id,
#   queue and time with an empty ``task_name`` / ``error_excerpt``.
# - RabbitMQ: ``total`` only (the ``.XQ`` message count). AMQP has no
#   non-destructive read of queued messages, so ``entries`` is always empty
#   and ``next_cursor`` is ``None``. The dashboard should render the count
#   and say the listing is unavailable on this broker.
# - StubBroker: full entries from ``dead_letters_by_queue`` (tests).
#
# ``requeue_dead_letter`` stays absent: Dramatiq has no resurrect-by-id API.
DEAD_LETTER_LISTING_CAPABILITIES: frozenset[str] = frozenset({"list_dead_letters"})


__all__ = [
    "ABORTABLE_CAPABILITIES",
    "DEAD_LETTER_LISTING_CAPABILITIES",
    "DEFAULT_CAPABILITIES",
]
