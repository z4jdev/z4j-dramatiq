"""Dead-letter actions for Dramatiq: fail-closed requeue, read-only listing.

Stock Dramatiq exposes no recover-by-id or public resurrection API for
exhausted messages. It does not provide the ``DeadLetter``,
``get_dead_letter``, or ``resurrect`` interfaces earlier versions of this
adapter assumed, so :func:`requeue_dead_letter_action` refuses.

Listing is a different matter: both built-in brokers *keep* dead letters.
When a message exhausts its retries the broker ``nack``s it into the queue's
dead-letter queue, named ``<queue>.XQ`` (``dramatiq.common.xq_name``; the
``.DQ`` suffix is the *delayed* queue, not the dead-letter one). What can be
read back without consuming differs per broker:

- **Redis**: ``<namespace>:<queue>.XQ`` is a sorted set of message ids scored
  by the dead-letter time in milliseconds, and ``<namespace>:<queue>.XQ.msgs``
  a hash of id to encoded message. Both are plain reads (``ZREVRANGE`` /
  ``HMGET``), so the listing is complete: id, actor, time, attempts and the
  traceback the Retries middleware stored. Message bodies are parsed as JSON
  only; a deployment using Dramatiq's ``PickleEncoder`` gets id, queue and
  time but an empty ``task_name`` / ``error_excerpt``, because the agent
  never unpickles broker bytes.
- **RabbitMQ**: AMQP has no non-destructive read of a queue's messages
  (``basic_get`` consumes and ``nack(requeue)`` reorders), so the page
  carries only ``total`` (the ``.XQ`` message count from a passive declare)
  with an empty ``entries`` list and no cursor.
- **StubBroker** (tests): ``broker.dead_letters_by_queue``.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from z4j_core.errors import AdapterError, ValidationError, Z4JError
from z4j_core.models import (
    DLQ_LIST_MAX_LIMIT,
    CommandResult,
    DeadLetterEntry,
    DeadLetterPage,
    decode_offset_cursor,
    encode_offset_cursor,
    redact_error_excerpt,
)
from z4j_core.redaction.engine import RedactionEngine

from z4j_dramatiq._offload import OffloadTimeoutError, offload

logger = logging.getLogger("z4j.adapter.dramatiq.actions.dlq")

#: Cap on the synchronous broker reads behind a listing (pure-sync redis-py
#: / pika calls run on the offload pool so a broker stall cannot freeze the
#: agent's event loop).
_OFFLOAD_TIMEOUT = 10.0

#: Broker kinds :func:`list_dead_letters_action` can serve. Drives the
#: ``list_dead_letters`` capability promotion in the engine adapter.
LISTABLE_BROKER_KINDS: frozenset[str] = frozenset({"redis", "rabbitmq", "stub"})


async def requeue_dead_letter_action(
    broker: Any,
    *,
    task_id: str,
    actor_name: str | None = None,
    queue_name: str | None = None,
    override_args: tuple[Any, ...] | None = None,
    override_kwargs: dict[str, Any] | None = None,
) -> CommandResult:
    """Refuse an action stock Dramatiq cannot implement."""
    del broker, actor_name, queue_name, override_args, override_kwargs
    return CommandResult(
        status="failed",
        error=(
            f"cannot requeue {task_id!r}: stock Dramatiq exposes no recoverable dead-letter API"
        ),
    )


# ---------------------------------------------------------------------------
# list_dead_letters
# ---------------------------------------------------------------------------


def broker_kind(broker: Any) -> str:
    """``redis`` / ``rabbitmq`` / ``stub`` / ``unknown`` by class ancestry.

    Walks the MRO so a user subclass of ``RedisBroker`` is still ``redis``.
    """
    for klass in type(broker).__mro__:
        name = klass.__name__.lower()
        if "redis" in name:
            return "redis"
        if "rabbit" in name or "amqp" in name:
            return "rabbitmq"
        if "stub" in name:
            return "stub"
    return "unknown"


async def list_dead_letters_action(
    broker: Any,
    *,
    queue: str | None = None,
    limit: int = 100,
    cursor: str | None = None,
    redaction: RedactionEngine | None = None,
    engine_name: str = "dramatiq",
) -> DeadLetterPage:
    """Page the broker's dead-letter queues, newest first (see module docs).

    ``queue`` names a canonical queue (``.DQ`` / ``.XQ`` suffixes are
    stripped); ``None`` spans every queue the broker has declared. ``cursor``
    is a decimal offset (:func:`encode_offset_cursor`).

    Raises :class:`ValidationError` for a malformed cursor and
    :class:`AdapterError` for an unsupported broker, an unreachable broker,
    or a read that outlives its timeout.
    """
    try:
        offset = decode_offset_cursor(cursor)
    except ValueError as exc:
        raise ValidationError(str(exc)) from exc
    bounded = max(1, min(int(limit), DLQ_LIST_MAX_LIMIT))
    scrubber = redaction or RedactionEngine()
    kind = broker_kind(broker)
    if kind == "stub":
        return _list_stub(broker, queue, bounded, offset, scrubber, engine_name)
    if kind == "redis":
        reader = _list_redis
    elif kind == "rabbitmq":
        reader = _count_rabbitmq
    else:
        raise AdapterError(
            f"list_dead_letters is not supported for broker {type(broker).__name__!r}: "
            "z4j can read dead letters from Dramatiq's Redis and RabbitMQ brokers only",
        )
    try:
        return await offload(
            reader,
            broker,
            queue,
            bounded,
            offset,
            scrubber,
            engine_name,
            timeout=_OFFLOAD_TIMEOUT,
        )
    except OffloadTimeoutError as exc:
        raise AdapterError(
            f"list_dead_letters timed out after {_OFFLOAD_TIMEOUT:g}s waiting on the broker",
        ) from exc
    except Z4JError:
        raise
    except Exception as exc:
        raise AdapterError(f"list_dead_letters failed: {exc}") from exc


def _canonical(queue_name: str) -> str:
    if queue_name.endswith((".DQ", ".XQ")):
        return queue_name[:-3]
    return queue_name


def _queue_names(broker: Any, queue: str | None) -> list[str]:
    if queue is not None:
        return [_canonical(queue)]
    declared: set[str] = set()
    getter = getattr(broker, "get_declared_queues", None)
    if callable(getter):
        try:
            declared.update(str(name) for name in getter())
        except Exception:
            logger.debug("z4j dramatiq: get_declared_queues failed", exc_info=True)
    by_queue = getattr(broker, "dead_letters_by_queue", None)
    if isinstance(by_queue, dict):
        declared.update(str(name) for name in by_queue)
    return sorted({_canonical(name) for name in declared if name})


def _list_redis(
    broker: Any,
    queue: str | None,
    limit: int,
    offset: int,
    redaction: RedactionEngine,
    engine_name: str,
) -> DeadLetterPage:
    client = broker.client
    namespace = str(getattr(broker, "namespace", "dramatiq"))
    fetch_end = offset + limit
    total = 0
    # (dead-letter time in ms, message id, queue); newest first.
    candidates: list[tuple[float, str, str]] = []
    for name in _queue_names(broker, queue):
        xq_key = f"{namespace}:{name}.XQ"
        total += int(client.zcard(xq_key))
        for member, score in client.zrevrange(xq_key, 0, fetch_end - 1, withscores=True):
            candidates.append((float(score), _as_text(member), name))
    candidates.sort(key=lambda item: (-item[0], item[2], item[1]))
    window = candidates[offset:fetch_end]

    # One HMGET per queue for the message bodies in the window.
    ids_by_queue: dict[str, list[str]] = {}
    for _score, message_id, name in window:
        ids_by_queue.setdefault(name, []).append(message_id)
    bodies: dict[tuple[str, str], dict[str, Any] | None] = {}
    for name, ids in ids_by_queue.items():
        try:
            raw_bodies = list(client.hmget(f"{namespace}:{name}.XQ.msgs", ids))
        except Exception:
            raw_bodies = [None] * len(ids)
        for message_id, raw in zip(ids, raw_bodies, strict=False):
            bodies[(name, message_id)] = _parse_message_json(raw)

    entries = [
        _entry_from_fields(
            message_id,
            name,
            bodies.get((name, message_id)),
            datetime.fromtimestamp(score / 1000.0, tz=UTC),
            redaction,
        )
        for score, message_id, name in window
    ]
    next_cursor = encode_offset_cursor(fetch_end) if fetch_end < total else None
    return DeadLetterPage(entries=entries, next_cursor=next_cursor, total=total, engine=engine_name)


def _count_rabbitmq(
    broker: Any,
    queue: str | None,
    limit: int,
    offset: int,
    redaction: RedactionEngine,
    engine_name: str,
) -> DeadLetterPage:
    """RabbitMQ: no non-destructive read exists, so report the count only."""
    del limit, offset, redaction
    total = 0
    for name in _queue_names(broker, queue):
        counts = broker.get_queue_message_counts(name)
        # (ready, delayed, dead-lettered)
        total += int(counts[2]) if isinstance(counts, (tuple, list)) and len(counts) >= 3 else 0
    return DeadLetterPage(entries=[], next_cursor=None, total=total, engine=engine_name)


def _list_stub(
    broker: Any,
    queue: str | None,
    limit: int,
    offset: int,
    redaction: RedactionEngine,
    engine_name: str,
) -> DeadLetterPage:
    by_queue = getattr(broker, "dead_letters_by_queue", None) or {}
    collected: list[tuple[str, Any]] = []
    for name in _queue_names(broker, queue):
        messages = list(by_queue.get(name, []))
        # Appended in nack order; newest last.
        collected.extend((name, message) for message in reversed(messages))
    total = len(collected)
    fetch_end = offset + limit
    entries = [
        _entry_from_fields(
            str(getattr(message, "message_id", "")),
            name,
            _fields_from_message(message),
            None,
            redaction,
        )
        for name, message in collected[offset:fetch_end]
    ]
    next_cursor = encode_offset_cursor(fetch_end) if fetch_end < total else None
    return DeadLetterPage(entries=entries, next_cursor=next_cursor, total=total, engine=engine_name)


def _fields_from_message(message: Any) -> dict[str, Any]:
    options = getattr(message, "options", None)
    return {
        "actor_name": getattr(message, "actor_name", ""),
        "queue_name": getattr(message, "queue_name", ""),
        "options": dict(options) if isinstance(options, dict) else {},
    }


def _parse_message_json(raw: Any) -> dict[str, Any] | None:
    """Decode a stored message body as JSON only; never the global encoder."""
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError, UnicodeDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _entry_from_fields(
    message_id: str,
    queue_name: str,
    fields: dict[str, Any] | None,
    failed_at: datetime | None,
    redaction: RedactionEngine,
) -> DeadLetterEntry:
    fields = fields or {}
    options = fields.get("options")
    options = options if isinstance(options, dict) else {}
    retries = options.get("retries")
    # Retries increments ``options["retries"]`` before checking exhaustion, so
    # after the final failure it equals the number of executions that failed.
    attempts = retries if isinstance(retries, int) and not isinstance(retries, bool) else None
    actor_name = fields.get("actor_name")
    task_name = _scrub_text(actor_name if isinstance(actor_name, str) else "", redaction)[:500]
    return DeadLetterEntry(
        task_id=message_id or "?",
        task_name=task_name,
        queue=queue_name,
        failed_at=failed_at,
        error_excerpt=redact_error_excerpt(options.get("traceback"), redaction),
        attempts=attempts if attempts is None or attempts >= 0 else None,
    )


def _scrub_text(value: str, redaction: RedactionEngine) -> str:
    if not value:
        return ""
    scrubbed = redaction.scrub(value)
    return scrubbed if isinstance(scrubbed, str) else str(scrubbed)


def _as_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


__all__ = [
    "LISTABLE_BROKER_KINDS",
    "broker_kind",
    "list_dead_letters_action",
    "requeue_dead_letter_action",
]
