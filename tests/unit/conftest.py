"""Shared fixtures for z4j-dramatiq unit tests.

Fakes here let the entire z4j-dramatiq surface run without
``dramatiq`` actually installed. Integration tests use a real
broker (Redis OR RabbitMQ - both are mandatory matrix legs per
multi-engine plan §13.3).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import pytest


@dataclass
class FakeMessage:
    """Minimal stand-in for ``dramatiq.Message``."""

    message_id: str
    actor_name: str = "myapp.tasks.work"
    queue_name: str = "default"
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)
    message_timestamp: int = 0


@dataclass
class FakeActor:
    """Minimal stand-in for a ``@dramatiq.actor``."""

    actor_name: str = "myapp.tasks.work"
    queue_name: str = "default"
    sent: list[dict[str, Any]] = field(default_factory=list)
    fn: Any = None

    def send(self, *args: Any, **kwargs: Any) -> FakeMessage:
        msg = FakeMessage(
            message_id=f"msg-{len(self.sent) + 1}",
            actor_name=self.actor_name,
            queue_name=self.queue_name,
            args=tuple(args),
            kwargs=dict(kwargs),
        )
        self.sent.append({"args": tuple(args), "kwargs": dict(kwargs)})
        return msg

    def send_with_options(
        self,
        *,
        args: Iterable[Any] = (),
        kwargs: dict[str, Any] | None = None,
        queue_name: str | None = None,
        **_extra: Any,
    ) -> FakeMessage:
        actual_q = queue_name or self.queue_name
        msg = FakeMessage(
            message_id=f"msg-{len(self.sent) + 1}",
            actor_name=self.actor_name,
            queue_name=actual_q,
            args=tuple(args),
            kwargs=dict(kwargs or {}),
        )
        self.sent.append(
            {
                "args": tuple(args),
                "kwargs": dict(kwargs or {}),
                "queue_name": actual_q,
            }
        )
        return msg


@dataclass
class FakeBroker:
    """Duck-typed broker. Production passes a real Dramatiq Broker."""

    actors: dict[str, FakeActor] = field(default_factory=dict)
    middleware: list[Any] = field(default_factory=list)
    purged: list[str] = field(default_factory=list)
    queue_counts: dict[str, int] = field(default_factory=dict)

    def add_middleware(self, mw: Any) -> None:
        self.middleware.append(mw)

    def get_actor(self, name: str) -> FakeActor:
        if name not in self.actors:
            raise KeyError(name)
        return self.actors[name]

    def register(self, actor: FakeActor) -> None:
        self.actors[actor.actor_name] = actor

    def get_queue_message_counts(
        self,
        queue_name: str,
    ) -> tuple[int, int, int]:
        return (self.queue_counts.get(queue_name, 0), 0, 0)

    def flush(self, queue_name: str) -> None:
        self.purged.append(queue_name)
        self.queue_counts[queue_name] = 0


try:
    # Only importable when the ``abort`` extra is present (e.g. under
    # ``uv sync --all-extras``); absent in the minimal test env.
    from dramatiq_abort import Abortable as _RealAbortable
except ImportError:  # pragma: no cover - depends on installed extras
    _RealAbortable = object  # type: ignore[assignment, misc]


class Abortable(_RealAbortable):  # type: ignore[valid-type, misc]
    """Stand-in for the ``Abortable`` middleware.

    Capability promotion requires the real ``dramatiq-abort`` package and an
    ``isinstance(mw, dramatiq_abort.Abortable)`` match. Subclassing the real
    class keeps this fake on that production detection path when the optional
    test extra is installed.

    ``__init__`` is a deliberate no-op: the real
    ``dramatiq_abort.Abortable`` requires an event-backend argument
    the fake has no use for, and detection only ever inspects the
    type, never runs abort logic against the instance.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Do NOT chain to super().__init__(): the real Abortable needs
        # a backend and performs setup this fake middleware never uses.
        pass


@pytest.fixture
def broker() -> FakeBroker:
    b = FakeBroker()
    b.register(FakeActor(actor_name="myapp.tasks.send_email"))
    b.queue_counts["default"] = 3
    return b


@pytest.fixture
def broker_with_abortable(broker: FakeBroker, monkeypatch: pytest.MonkeyPatch) -> FakeBroker:
    """Broker that has Abortable middleware in its stack."""
    middleware = Abortable()
    broker.add_middleware(middleware)

    # Minimal test environments intentionally omit the optional package. Patch
    # the same lazy type loader production uses so this fixture represents an
    # installed extra with an exact type match, never a class-name fallback.
    from z4j_dramatiq import engine as engine_module

    monkeypatch.setattr(engine_module, "_load_abortable_type", lambda: type(middleware))
    return broker


@pytest.fixture
def message() -> FakeMessage:
    return FakeMessage(message_id="msg-1")
