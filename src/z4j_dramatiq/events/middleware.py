"""The Z4JMiddleware that captures Dramatiq lifecycle events.

Dramatiq's middleware chain is its blessed observability hook -
the same surface ``CurrentMessage``, ``Retries``, and ``Abortable``
use. Adding our middleware to the user's broker is the *correct*
integration path; no monkey-patching needed.

Lifecycle hooks we use:

- ``after_enqueue`` → ``task.received``
- ``before_process_message`` → ``task.started``
- ``after_process_message`` → ``task.succeeded`` if ``exception is None``,
  else ``task.failed``

Hooks we deliberately do NOT use:

- ``before_enqueue`` - fires before message_id is finalized; we'd
  miss ``task_id`` correlation.
- ``before_worker_boot`` / ``after_worker_boot`` - worker lifecycle,
  surfaced via the heartbeat instead.

Safety properties (CLAUDE.md §2.2):

- Every hook runs through a top-level try/except. A bug in our
  code cannot crash the user's worker.
- The middleware does not deserialize args/kwargs. Mapper drops
  them by design (CLAUDE.md §2.3).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from z4j_core.models import Event, EventKind
from z4j_core.redaction.engine import RedactionEngine

from z4j_dramatiq.events.mapper import build_event

logger = logging.getLogger("z4j.agent.dramatiq.middleware")


# Subclass dramatiq.Middleware lazily - many test environments
# do not have the ``dramatiq`` package importable. The engine
# adapter only constructs Z4JMiddleware when it is wiring into a
# real broker; tests can construct a duck-typed instance directly
# (see TestZ4JMiddleware in tests/unit/test_middleware.py).
def _resolve_base() -> type:
    try:
        from dramatiq.middleware import Middleware  # type: ignore[import-not-found]
    except ImportError:
        # Fallback so the class is importable even without dramatiq -
        # callers that try to register the middleware on a real broker
        # will get a clear ImportError when they construct it.
        class _StubBase:
            pass
        return _StubBase
    return Middleware


_Base = _resolve_base()


class Z4JMiddleware(_Base):  # type: ignore[misc, valid-type]
    """Dramatiq middleware that emits z4j events for every message.

    Args:
        sink: Callable invoked with each :class:`Event` produced.
              The engine adapter wires this to its internal asyncio
              queue via ``call_soon_threadsafe``.
        redaction: Shared :class:`RedactionEngine`. The agent
                   runtime's own engine is passed in production.
    """

    def __init__(
        self,
        *,
        sink: Callable[[Event], None],
        redaction: RedactionEngine,
    ) -> None:
        self._sink = sink
        self._redaction = redaction
        # The Dramatiq base class historically does not have an
        # __init__ but recent versions accept arbitrary kwargs; do
        # not call super().__init__ - we have nothing to forward.

    # ------------------------------------------------------------------
    # Dramatiq middleware hooks
    # ------------------------------------------------------------------

    def after_enqueue(
        self,
        broker: Any,  # noqa: ARG002
        message: Any,
        delay: int | None,  # noqa: ARG002
    ) -> None:
        """Message has been written to the broker - emit ``task.received``."""
        self._safe_emit(EventKind.TASK_RECEIVED, message)

    def before_process_message(self, broker: Any, message: Any) -> None:  # noqa: ARG002
        """Worker has fetched the message and is about to run it."""
        self._safe_emit(EventKind.TASK_STARTED, message)

    def after_process_message(
        self,
        broker: Any,  # noqa: ARG002
        message: Any,
        *,
        result: Any = None,  # noqa: ARG002
        exception: BaseException | None = None,
    ) -> None:
        """Worker has finished - success or failure based on ``exception``."""
        kind = (
            EventKind.TASK_FAILED if exception is not None
            else EventKind.TASK_SUCCEEDED
        )
        self._safe_emit(kind, message, exception=exception)

    # ------------------------------------------------------------------
    # Internal: build + sink without ever raising into Dramatiq
    # ------------------------------------------------------------------

    def _safe_emit(
        self,
        kind: EventKind,
        message: Any,
        exception: BaseException | None = None,
    ) -> None:
        sink = self._sink
        redaction = self._redaction
        try:
            event = build_event(
                kind=kind,
                message=message,
                redaction=redaction,
                actor=_resolve_actor(message),
                exception=exception,
            )
            sink(event)
        except Exception:  # noqa: BLE001
            logger.exception(
                "z4j dramatiq: middleware emit raised - dropping event "
                "(this is a bug in z4j, NOT in your task code)",
            )


def _resolve_actor(message: Any) -> Any | None:
    """Best-effort lookup of the actor for ``message``.

    Dramatiq stores every registered actor on the broker. If the
    user passes us a Message we can resolve back to the actor (and
    therefore to ``@z4j_meta`` decorations) via the global broker
    registry. Failures are silent - we just drop the meta lookup.
    """
    try:
        from dramatiq import get_broker  # type: ignore[import-not-found]
    except ImportError:
        return None
    try:
        broker = get_broker()
        actor_name = getattr(message, "actor_name", None)
        if actor_name is None:
            return None
        return broker.get_actor(actor_name)
    except Exception:  # noqa: BLE001
        return None


__all__ = ["Z4JMiddleware"]
