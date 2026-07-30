"""The :class:`DramatiqEngineAdapter` - z4j's Dramatiq engine adapter."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from typing import Any

from z4j_core.models import (
    CommandResult,
    DiscoveryHints,
    Event,
    Queue,
    Task,
    TaskDefinition,
    TaskRegistryDelta,
    Worker,
)
from z4j_core.redaction.engine import RedactionEngine
from z4j_core.version import PROTOCOL_VERSION

from z4j_dramatiq.actions import (
    bulk_retry_action,
    cancel_task_action,
    purge_queue_action,
    requeue_dead_letter_action,
    retry_task_action,
)
from z4j_dramatiq.capabilities import (
    ABORTABLE_CAPABILITIES,
    DEFAULT_CAPABILITIES,
)
from z4j_dramatiq.discovery import discover_runtime
from z4j_dramatiq.events.mapper import DRAMATIQ_ENGINE_NAME
from z4j_dramatiq.events.middleware import Z4JMiddleware

logger = logging.getLogger("z4j.adapter.dramatiq.engine")


class DramatiqEngineAdapter:
    """Queue-engine adapter for Dramatiq.

    Args:
        broker: Live Dramatiq Broker (Redis or RabbitMQ).
        redaction: Optional shared :class:`RedactionEngine`.
    """

    name: str = DRAMATIQ_ENGINE_NAME
    protocol_version: str = PROTOCOL_VERSION

    #: P1-1: attests this adapter implements the 1.7.1 safe-retry contract --
    #: retry_task STRIPS the smuggled ``__z4j_actor_name__`` / ``__z4j_queue_name__``
    #: control keys from override_kwargs and re-runs the ORIGINAL message by
    #: reference (DLQ resurrect), failing closed rather than re-sending the actor
    #: with an empty payload. The z4j-bare dispatcher reads this flag to refuse a
    #: retry when paired with a pre-1.7.1 adapter that lacks it.
    safe_retry_by_reference: bool = True

    def __init__(
        self,
        *,
        broker: Any,
        redaction: RedactionEngine | None = None,
    ) -> None:
        self.broker = broker
        self.redaction = redaction or RedactionEngine()
        self._event_queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=10_000)
        self._middleware: Z4JMiddleware | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect_signals(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Install :class:`Z4JMiddleware` into the user's broker."""
        target_loop = loop
        if target_loop is None:
            try:
                target_loop = asyncio.get_running_loop()
            except RuntimeError:
                target_loop = None
        self._loop = target_loop

        def sink(event: Event) -> None:
            current_loop = self._loop
            if current_loop is None:
                try:
                    current_loop = asyncio.get_running_loop()
                except RuntimeError:
                    logger.debug("z4j dramatiq: no running loop; dropping event")
                    return
            current_loop.call_soon_threadsafe(self._enqueue_event, event)

        self._middleware = Z4JMiddleware(sink=sink, redaction=self.redaction)
        try:
            self.broker.add_middleware(self._middleware)
            logger.info("z4j dramatiq: middleware installed on broker")
        except Exception:
            logger.exception(
                "z4j dramatiq: broker.add_middleware raised - capture inactive",
            )

    def disconnect_signals(self) -> None:
        """Remove the middleware from the broker. Idempotent."""
        if self._middleware is None:
            return
        middleware = getattr(self.broker, "middleware", None)
        if isinstance(middleware, list):
            with contextlib.suppress(ValueError):
                middleware.remove(self._middleware)
        self._middleware = None
        self._loop = None

    def _enqueue_event(self, event: Event) -> None:
        """Push an Event onto the internal queue, dropping oldest when full."""
        for _attempt in range(3):
            try:
                self._event_queue.put_nowait(event)
                return
            except asyncio.QueueFull:
                try:
                    dropped = self._event_queue.get_nowait()
                    logger.warning(
                        "z4j dramatiq: event queue full, dropped event kind=%s",
                        getattr(dropped, "kind", "?"),
                    )
                except asyncio.QueueEmpty:
                    pass
        logger.error(
            "z4j dramatiq: failed to enqueue event after retries kind=%s",
            getattr(event, "kind", "?"),
        )

    # ------------------------------------------------------------------
    # QueueEngineAdapter - discovery + observation
    # ------------------------------------------------------------------

    async def discover_tasks(
        self,
        hints: DiscoveryHints | None = None,
    ) -> list[TaskDefinition]:
        return discover_runtime(self.broker)

    async def subscribe_registry_changes(
        self,
    ) -> AsyncIterator[TaskRegistryDelta]:
        """Block forever - Dramatiq has no registry-change signal."""
        return
        yield  # pragma: no cover  (makes this a generator)

    async def subscribe_events(self) -> AsyncIterator[Event]:
        while True:
            event = await self._event_queue.get()
            yield event

    async def list_queues(self) -> list[Queue]:
        return []

    async def list_workers(self) -> list[Worker]:
        return []

    async def get_task(self, task_id: str) -> Task | None:
        # Brain owns authoritative task state. Adapter raises only
        # if it can prove the task never existed; here we cannot,
        # so we always return None (no error).
        del task_id
        return None

    async def reconcile_task(self, task_id: str) -> CommandResult:
        """Query Dramatiq's Results middleware for authoritative state.

        Dramatiq has no universal result backend - users opt in via
        the ``Results`` middleware with a specific backend (Redis,
        memcached, Stub). We check whether the broker has a
        Results middleware; if so, ask it; otherwise return
        ``engine_state="unknown"`` so the brain leaves its state alone.
        """
        middleware = getattr(self.broker, "middleware", None) or []
        results_mw = None
        for mw in middleware:
            if type(mw).__name__ == "Results":
                results_mw = mw
                break
        if results_mw is None:
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "unknown",
                    "finished_at": None,
                    "exception": "Results middleware not installed",
                },
            )
        backend = getattr(results_mw, "backend", None)
        if backend is None:
            return CommandResult(
                status="success",
                result={
                    "task_id": task_id,
                    "engine_state": "unknown",
                    "finished_at": None,
                    "exception": "Results backend not configured",
                },
            )
        # Dramatiq's Results backend keys by message_id and we don't
        # have the queue/actor context here, so we surface "unknown"
        # honestly rather than guess. The brain's snapshot stays
        # authoritative.
        return CommandResult(
            status="success",
            result={
                "task_id": task_id,
                "engine_state": "unknown",
                "finished_at": None,
                "exception": ("Dramatiq reconcile requires actor/queue context"),
            },
        )

    def get_health(self) -> dict[str, Any]:
        """Return broker connectivity + queue counts for the heartbeat."""
        broker_type = _broker_type(self.broker)
        health: dict[str, Any] = {
            "broker_type": broker_type,
            "broker_connected": False,
            "queue_depths": {},
            "abortable_installed": _has_abortable(self.broker),
        }
        try:
            # Both Redis + RabbitMQ brokers expose this method.
            queues = list(getattr(self.broker, "actors", {}).values())
            queue_names = sorted({getattr(a, "queue_name", "default") for a in queues})
            for q_name in queue_names:
                fn = getattr(self.broker, "get_queue_message_counts", None)
                if callable(fn):
                    try:
                        counts = fn(q_name)
                        if isinstance(counts, (tuple, list)) and counts:
                            health["queue_depths"][q_name] = int(counts[0] or 0)
                    except Exception:  # noqa: S110  best-effort queue depth
                        pass
            health["broker_connected"] = True
        except Exception as exc:
            health["broker_error"] = str(exc)[:200]
        return health

    # ------------------------------------------------------------------
    # QueueEngineAdapter - actions
    # ------------------------------------------------------------------

    async def submit_task(
        self,
        name: str,
        *,
        args: tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        queue: str | None = None,
        eta: float | None = None,
        priority: int | None = None,
    ) -> CommandResult:
        """Universal enqueue - sends a Dramatiq message via the
        registered actor's ``send_with_options`` if the actor is
        in-process, otherwise constructs a raw Message and uses
        ``broker.enqueue``.
        """
        try:
            import dramatiq

            actor = None
            try:
                actor = dramatiq.get_broker().get_actor(name)
            except Exception:
                actor = None
            if actor is not None:
                opts: dict[str, Any] = {}
                if queue:
                    opts["queue_name"] = queue
                msg = actor.send_with_options(
                    args=tuple(args),
                    kwargs=kwargs or {},
                    **opts,
                )
                new_id = getattr(msg, "message_id", None)
            else:
                # Fallback: construct a raw Message. The actor may
                # live in another process; the broker will route it
                # by name.
                from dramatiq.message import Message

                msg = Message(
                    queue_name=queue or "default",
                    actor_name=name,
                    args=tuple(args),
                    kwargs=kwargs or {},
                    options={},
                )
                self.broker.enqueue(msg)
                new_id = msg.message_id
        except Exception as exc:
            return CommandResult(status="failed", error=str(exc))
        return CommandResult(
            status="success",
            result={"task_id": new_id, "engine": self.name},
        )

    async def retry_task(
        self,
        task_id: str,
        *,
        override_args: tuple[Any, ...] | None = None,
        override_kwargs: dict[str, Any] | None = None,
        eta: float | None = None,
        priority: object = None,
    ) -> CommandResult:
        # The bare dispatcher smuggles the brain-supplied actor name (and
        # optional queue) through override_kwargs magic keys because this
        # retry_task signature pre-dates a task_name kwarg. Extract them on
        # a COPY, then if nothing genuine remains, collapse the dict back to
        # None. Otherwise a control-key-only override_kwargs would read as an
        # operator override downstream (has_overrides = override_kwargs is
        # not None) and SKIP the DLQ requeue-by-reference path, re-sending
        # the actor with an EMPTY payload instead of resurrecting the
        # original message (1.7.1 H2).
        actor_name: str | None = None
        queue_name: str | None = None
        if override_kwargs is not None:
            override_kwargs = dict(override_kwargs)
            actor_name = override_kwargs.pop("__z4j_actor_name__", None)
            queue_name = override_kwargs.pop("__z4j_queue_name__", None)
            if not override_kwargs:
                override_kwargs = None
        return await retry_task_action(
            self.broker,
            task_id=task_id,
            actor_name=actor_name,
            queue_name=queue_name,
            override_args=override_args,
            override_kwargs=override_kwargs,
            eta=eta,
            priority=priority,
        )

    async def cancel_task(self, task_id: str) -> CommandResult:
        return await cancel_task_action(self.broker, task_id=task_id)

    async def purge_queue(
        self,
        queue_name: str,
        *,
        confirm_token: str | None = None,
        force: bool = False,
    ) -> CommandResult:
        return await purge_queue_action(
            self.broker,
            queue_name=queue_name,
            confirm_token=confirm_token,
            force=force,
        )

    # Honest absences (mirror the RQ pattern). Each returns a clear
    # failure message instead of silently no-op'ing.

    async def bulk_retry(
        self,
        filter: dict[str, Any],  # noqa: A002
        *,
        max: int = 1000,  # noqa: A002
    ) -> CommandResult:
        return await bulk_retry_action(
            self.broker,
            filter=filter,
            max=max,
        )

    async def requeue_dead_letter(self, task_id: str) -> CommandResult:
        # The brain's bulk-retry / DLQ path enriches the filter /
        # body with actor_name + queue_name from its Message snapshot;
        # the per-task DLQ endpoint historically passed only
        # ``task_id`` so callers that haven't updated their brain
        # still hit the fallback-with-clear-error path.
        return await requeue_dead_letter_action(
            self.broker,
            task_id=task_id,
        )

    async def rate_limit(
        self,
        task_name: str,
        rate: str,
        *,
        worker_name: str | None = None,
    ) -> CommandResult:
        return CommandResult(
            status="failed",
            error=(
                "rate_limit is not implemented for the Dramatiq engine. "
                "Use Dramatiq's own Throttler middleware to rate-limit "
                "actors in your worker configuration."
            ),
        )

    async def restart_worker(self, worker_id: str) -> CommandResult:
        return CommandResult(
            status="failed",
            error=(
                "restart_worker is not supported by the Dramatiq engine "
                "- Dramatiq workers expose no remote-control channel. "
                "Restart the worker process out-of-band (systemd, k8s, etc.)."
            ),
        )

    # ------------------------------------------------------------------
    # Capabilities - promoted to ABORTABLE_CAPABILITIES iff the broker
    # has the Abortable middleware installed.
    # ------------------------------------------------------------------

    def capabilities(self) -> set[str]:
        caps = set(
            ABORTABLE_CAPABILITIES if _has_abortable(self.broker) else DEFAULT_CAPABILITIES,
        )
        # M3: bulk_retry + requeue_dead_letter recover a failed message BY
        # REFERENCE through the dead-letter queue, which a stock dramatiq
        # broker does NOT provide. Advertise them only when a DeadLetter
        # middleware is present; otherwise every id would fail closed, so the
        # button must not appear. (retry_task itself stays -- it works from
        # operator overrides via the actor.)
        if not _has_dlq(self.broker):
            caps -= {"bulk_retry", "requeue_dead_letter"}
        return caps


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _broker_type(broker: Any) -> str:
    """Return ``redis`` / ``rabbitmq`` / ``stub`` / ``unknown`` for a broker."""
    cls_name = type(broker).__name__.lower()
    if "redis" in cls_name:
        return "redis"
    if "rabbit" in cls_name or "amqp" in cls_name:
        return "rabbitmq"
    if "stub" in cls_name:
        return "stub"
    return "unknown"


def _has_abortable(broker: Any) -> bool:
    middleware = getattr(broker, "middleware", None)
    if not middleware:
        return False
    try:
        # Abortable ships in the separate dramatiq-abort package,
        # not dramatiq itself.
        from dramatiq_abort import (  # type: ignore[import-not-found]
            Abortable,
        )
    except ImportError:
        # dramatiq-abort not installed; fall back to a structural
        # check on the class name.
        return any(type(mw).__name__ == "Abortable" for mw in middleware)
    return any(isinstance(mw, Abortable) for mw in middleware)


def _has_dlq(broker: Any) -> bool:
    """True iff the broker exposes the FULL native dead-letter recovery path
    that the by-reference retry actions (bulk_retry / requeue_dead_letter)
    actually drive in ``actions/dlq.py._try_native_dlq``: a callable
    ``broker.get_dead_letter`` fetcher AND a DeadLetter middleware with a
    callable ``resurrect``.

    RM2: a class-name match alone over-reports -- a broker missing
    ``get_dead_letter`` (or a DeadLetter middleware whose ``resurrect`` is not
    callable) would advertise the bulk_retry / requeue_dead_letter buttons,
    then have every id fall through to fail-closed at runtime. Gate the
    capability on the methods the recovery path really needs so the button
    only appears when it can work."""
    if not callable(getattr(broker, "get_dead_letter", None)):
        return False
    middleware = getattr(broker, "middleware", None) or []
    # dramatiq:473: match a DeadLetter subclass / proxy across the MRO (with a
    # callable resurrect), not only the exact leaf class name. Shared with the
    # runtime recovery path so the advertised capability and the path agree.
    from z4j_dramatiq.actions.dlq import is_dead_letter_middleware

    return any(is_dead_letter_middleware(mw) for mw in middleware)


__all__ = ["DramatiqEngineAdapter"]
