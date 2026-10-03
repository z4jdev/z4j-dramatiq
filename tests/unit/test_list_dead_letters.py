"""``list_dead_letters`` over Dramatiq's ``<queue>.XQ`` dead-letter store.

Three broker shapes are covered: the real ``StubBroker`` (a worker actually
dead-letters a message), a redis-py stand-in reproducing the RedisBroker key
layout, and a RabbitMQ stand-in where only the count is readable.
"""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from typing import Any

import pytest
from z4j_core.errors import AdapterError, ValidationError
from z4j_core.models import DeadLetterPage
from z4j_core.redaction import REDACTED, RedactionEngine
from z4j_dramatiq.actions.dlq import broker_kind, list_dead_letters_action
from z4j_dramatiq.capabilities import DEFAULT_CAPABILITIES
from z4j_dramatiq.engine import DramatiqEngineAdapter

# ---------------------------------------------------------------------------
# Real StubBroker
# ---------------------------------------------------------------------------


@pytest.fixture
def stub_broker():
    dramatiq = pytest.importorskip("dramatiq")
    from dramatiq.brokers.stub import StubBroker

    saved = dramatiq.broker.global_broker
    broker = StubBroker()
    dramatiq.broker.global_broker = broker
    try:
        yield broker
    finally:
        dramatiq.broker.global_broker = saved


class TestStubBroker:
    def test_kind_and_capability(self, stub_broker) -> None:
        assert broker_kind(stub_broker) == "stub"
        caps = DramatiqEngineAdapter(broker=stub_broker).capabilities()
        assert "list_dead_letters" in caps
        assert "requeue_dead_letter" not in caps
        assert caps == set(DEFAULT_CAPABILITIES) | {"list_dead_letters"}

    async def test_worker_dead_letter_is_listed_with_traceback_and_attempts(
        self, stub_broker
    ) -> None:
        import dramatiq
        from dramatiq.worker import Worker

        ran = threading.Event()

        @dramatiq.actor(
            broker=stub_broker,
            actor_name="z4j.test.dlq.explode",
            max_retries=1,
            min_backoff=1,
            max_backoff=2,
        )
        def explode():
            ran.set()
            raise RuntimeError("token=AKIAIOSFODNN7EXAMPLE rejected")

        worker = Worker(stub_broker, worker_timeout=100)
        worker.start()
        try:
            message = explode.send()
            stub_broker.join(explode.queue_name, fail_fast=False, timeout=30_000)
            worker.join()
        finally:
            worker.stop()
        assert ran.is_set()

        adapter = DramatiqEngineAdapter(broker=stub_broker, redaction=RedactionEngine())
        page = await adapter.list_dead_letters()
        assert page.engine == "dramatiq"
        assert page.total == 1
        assert page.next_cursor is None
        entry = page.entries[0]
        assert entry.task_id == message.message_id
        assert entry.task_name == "z4j.test.dlq.explode"
        assert entry.queue == "default"
        # One initial run plus one retry before Retries gave up.
        assert entry.attempts == 2
        assert entry.failed_at is None  # the StubBroker records no dead-letter time
        # The traceback carried a token-shaped secret; redaction scrubbed it.
        assert entry.error_excerpt == REDACTED

    async def test_seeded_entries_are_newest_first_and_paged(self, stub_broker) -> None:
        from dramatiq.message import Message

        stub_broker.declare_queue("emails")
        for i in range(3):
            stub_broker.dead_letters_by_queue["emails"].append(
                Message(
                    queue_name="emails",
                    actor_name="myapp.tasks.send",
                    args=(),
                    kwargs={},
                    options={"retries": 3, "traceback": f"ValueError: boom {i}"},
                    message_id=f"m-{i}",
                )
            )
        first = await list_dead_letters_action(stub_broker, limit=2)
        assert [e.task_id for e in first.entries] == ["m-2", "m-1"]
        assert first.total == 3
        assert first.next_cursor == "2"
        assert first.entries[0].error_excerpt == "ValueError: boom 2"
        assert first.entries[0].attempts == 3

        rest = await list_dead_letters_action(stub_broker, limit=2, cursor=first.next_cursor)
        assert [e.task_id for e in rest.entries] == ["m-0"]
        assert rest.next_cursor is None

    async def test_queue_filter_and_suffix_canonicalisation(self, stub_broker) -> None:
        from dramatiq.message import Message

        stub_broker.declare_queue("a")
        stub_broker.declare_queue("b")
        for name in ("a", "b"):
            stub_broker.dead_letters_by_queue[name].append(
                Message(
                    queue_name=name,
                    actor_name="x",
                    args=(),
                    kwargs={},
                    options={},
                    message_id=f"{name}-1",
                )
            )
        page = await list_dead_letters_action(stub_broker, queue="b.XQ")
        assert [e.task_id for e in page.entries] == ["b-1"]
        assert page.total == 1
        page = await list_dead_letters_action(stub_broker, queue="a.DQ")
        assert [e.task_id for e in page.entries] == ["a-1"]

    async def test_declared_queue_without_dead_letters_is_empty(self, stub_broker) -> None:
        stub_broker.declare_queue("quiet")
        page = await list_dead_letters_action(stub_broker)
        assert page.entries == []
        assert page.total == 0


# ---------------------------------------------------------------------------
# Redis broker key layout, hermetic
# ---------------------------------------------------------------------------


class _FakeRedisClient:
    def __init__(self) -> None:
        self.zsets: dict[str, dict[str, float]] = {}
        self.hashes: dict[str, dict[str, bytes]] = {}
        self.fail_with: Exception | None = None

    def zcard(self, key: str) -> int:
        if self.fail_with is not None:
            raise self.fail_with
        return len(self.zsets.get(key, {}))

    def zrevrange(self, key: str, start: int, end: int, withscores: bool = False) -> list[Any]:
        items = sorted(self.zsets.get(key, {}).items(), key=lambda kv: (-kv[1], kv[0]))
        window = items[start : None if end == -1 else end + 1]
        return (
            [(m.encode(), s) for m, s in window] if withscores else [m.encode() for m, _ in window]
        )

    def hmget(self, key: str, fields: list[str]) -> list[bytes | None]:
        stored = self.hashes.get(key, {})
        return [stored.get(f) for f in fields]


class FakeRedisBroker:
    """Named so ``broker_kind`` classifies it as redis; mirrors the attributes used."""

    def __init__(self) -> None:
        self.client = _FakeRedisClient()
        self.namespace = "dramatiq"
        self.queues: set[str] = set()
        self.middleware: list[Any] = []

    def get_declared_queues(self) -> set[str]:
        return set(self.queues)

    def seed(
        self,
        queue: str,
        message_id: str,
        at_ms: int,
        *,
        body: bytes | None = None,
        actor_name: str = "myapp.tasks.work",
        retries: int = 4,
        traceback: str = "Traceback...\nValueError: boom",
    ) -> None:
        self.queues.add(queue)
        self.queues.add(f"{queue}.DQ")  # RedisBroker declares the delayed queue too
        self.client.zsets.setdefault(f"{self.namespace}:{queue}.XQ", {})[message_id] = float(at_ms)
        if body is None:
            body = json.dumps(
                {
                    "queue_name": queue,
                    "actor_name": actor_name,
                    "args": [],
                    "kwargs": {},
                    "options": {"retries": retries, "traceback": traceback},
                    "message_id": message_id,
                    "message_timestamp": at_ms - 60_000,
                }
            ).encode()
        self.client.hashes.setdefault(f"{self.namespace}:{queue}.XQ.msgs", {})[message_id] = body


class TestRedisBroker:
    def test_kind_and_capability(self) -> None:
        broker = FakeRedisBroker()
        assert broker_kind(broker) == "redis"
        assert "list_dead_letters" in DramatiqEngineAdapter(broker=broker).capabilities()

    async def test_lists_newest_first_with_time_attempts_and_excerpt(self) -> None:
        broker = FakeRedisBroker()
        broker.seed("default", "old", 1_700_000_000_000)
        broker.seed("default", "new", 1_700_000_060_000, retries=2, traceback="x\nKeyError: 'k'")
        broker.seed("emails", "mail", 1_700_000_030_000, actor_name="myapp.tasks.send")

        page = await list_dead_letters_action(broker, redaction=RedactionEngine())
        assert isinstance(page, DeadLetterPage)
        assert page.total == 3
        assert [(e.queue, e.task_id) for e in page.entries] == [
            ("default", "new"),
            ("emails", "mail"),
            ("default", "old"),
        ]
        newest = page.entries[0]
        assert newest.task_name == "myapp.tasks.work"
        assert newest.failed_at == datetime.fromtimestamp(1_700_000_060, tz=UTC)
        assert newest.attempts == 2
        assert newest.error_excerpt == "x\nKeyError: 'k'"
        assert page.entries[1].task_name == "myapp.tasks.send"

    async def test_pages_with_offset_cursor(self) -> None:
        broker = FakeRedisBroker()
        for i in range(5):
            broker.seed("default", f"m-{i}", 1_000 + i)
        first = await list_dead_letters_action(broker, limit=2)
        assert [e.task_id for e in first.entries] == ["m-4", "m-3"]
        assert first.next_cursor == "2"
        last = await list_dead_letters_action(broker, limit=3, cursor=first.next_cursor)
        assert [e.task_id for e in last.entries] == ["m-2", "m-1", "m-0"]
        assert last.next_cursor is None

    async def test_queue_filter_ignores_other_queues(self) -> None:
        broker = FakeRedisBroker()
        broker.seed("default", "d", 1)
        broker.seed("emails", "e", 2)
        page = await list_dead_letters_action(broker, queue="emails")
        assert [e.task_id for e in page.entries] == ["e"]
        assert page.total == 1

    async def test_non_json_body_is_listed_without_decoding(self) -> None:
        broker = FakeRedisBroker()
        broker.seed("default", "pickled", 5, body=b"\x80\x04\x95pickle-bomb")
        page = await list_dead_letters_action(broker)
        entry = page.entries[0]
        assert entry.task_id == "pickled"
        assert entry.queue == "default"
        assert entry.failed_at is not None
        assert entry.task_name == ""
        assert entry.error_excerpt == ""
        assert entry.attempts is None

    async def test_excerpt_is_redacted(self) -> None:
        broker = FakeRedisBroker()
        broker.seed("default", "m", 1, traceback="boom token=AKIAIOSFODNN7EXAMPLE")
        page = await list_dead_letters_action(broker, redaction=RedactionEngine())
        assert page.entries[0].error_excerpt == REDACTED

    async def test_redis_failure_is_an_adapter_error(self) -> None:
        broker = FakeRedisBroker()
        broker.seed("default", "m", 1)
        broker.client.fail_with = ConnectionError("redis down")
        with pytest.raises(AdapterError, match="redis down"):
            await list_dead_letters_action(broker)

    async def test_malformed_cursor_is_a_validation_error(self) -> None:
        with pytest.raises(ValidationError, match="cursor"):
            await list_dead_letters_action(FakeRedisBroker(), cursor="nope")


# ---------------------------------------------------------------------------
# RabbitMQ: count only
# ---------------------------------------------------------------------------


class FakeRabbitmqBroker:
    def __init__(self) -> None:
        self.counts = {"default": (3, 0, 4), "emails": (0, 0, 1)}
        self.middleware: list[Any] = []
        self.probed: list[str] = []

    def get_declared_queues(self) -> set[str]:
        return set(self.counts)

    def get_queue_message_counts(self, queue_name: str) -> tuple[int, int, int]:
        self.probed.append(queue_name)
        return self.counts[queue_name]


class TestRabbitmqBroker:
    def test_kind_and_capability(self) -> None:
        broker = FakeRabbitmqBroker()
        assert broker_kind(broker) == "rabbitmq"
        assert "list_dead_letters" in DramatiqEngineAdapter(broker=broker).capabilities()

    async def test_reports_dead_letter_count_only(self) -> None:
        broker = FakeRabbitmqBroker()
        page = await list_dead_letters_action(broker)
        assert page.entries == []
        assert page.total == 5
        assert page.next_cursor is None
        assert sorted(broker.probed) == ["default", "emails"]

    async def test_queue_filter(self) -> None:
        page = await list_dead_letters_action(FakeRabbitmqBroker(), queue="emails")
        assert page.total == 1
        assert page.entries == []


# ---------------------------------------------------------------------------
# Unknown broker: honest absence
# ---------------------------------------------------------------------------


class TestUnknownBroker:
    def test_capability_not_advertised(self, broker) -> None:
        assert broker_kind(broker) == "unknown"
        assert "list_dead_letters" not in DramatiqEngineAdapter(broker=broker).capabilities()

    async def test_listing_refuses(self, broker) -> None:
        with pytest.raises(AdapterError, match="not supported"):
            await DramatiqEngineAdapter(broker=broker).list_dead_letters()

    def test_abortable_and_listing_promotions_compose(self, broker_with_abortable) -> None:
        # Abortable alone promotes cancel; listing still needs a readable broker.
        caps = DramatiqEngineAdapter(broker=broker_with_abortable).capabilities()
        assert "cancel_task" in caps
        assert "list_dead_letters" not in caps
