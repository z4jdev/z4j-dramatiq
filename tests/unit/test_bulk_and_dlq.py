"""Dramatiq adapter tests for ``bulk_retry`` + ``requeue_dead_letter``."""

from __future__ import annotations

import pytest
from z4j_dramatiq.actions.bulk_retry import bulk_retry_action
from z4j_dramatiq.actions.dlq import requeue_dead_letter_action


class DeadLetter:
    """Stand-in for ``dramatiq.middleware.DeadLetter``.

    The class NAME matters: ``_try_native_dlq`` matches the middleware
    via ``type(mw).__name__ == "DeadLetter"``.
    """

    def __init__(self, held: set[str] | None = None) -> None:
        self.resurrected: list[str] = []
        self._held = held if held is not None else set()

    def resurrect(self, task_id: str) -> None:
        # A real resurrect MOVES the message out of the dead-letter queue, so
        # it no longer appears in get_dead_letter. Model that so the RL1
        # after-check (which confirms the id is gone before claiming success)
        # sees the id removed.
        self.resurrected.append(task_id)
        self._held.discard(task_id)


def _enable_dlq(broker, held_ids):
    """Give a FakeBroker a dead-letter queue holding ``held_ids``."""
    held = set(held_ids)
    dl = DeadLetter(held)
    broker.middleware.append(dl)
    broker.get_dead_letter = lambda tid: [tid] if tid in held else []
    return dl


class TestBulkRetry:
    @pytest.mark.asyncio
    async def test_bulk_requeues_by_reference_via_dlq(self, broker):
        # 1.7.1 (CX-H5): bulk retry re-runs each id BY REFERENCE through
        # the dead-letter queue; it does NOT invoke a client-named actor.
        dl = _enable_dlq(broker, ["msg-1", "msg-2"])
        result = await bulk_retry_action(
            broker,
            filter={"task_ids": ["msg-1", "msg-2"]},
            max=10,
        )
        assert result.status == "success"
        assert result.result["retried"] == 2
        assert sorted(dl.resurrected) == ["msg-1", "msg-2"]
        # No actor was invoked (requeue-by-reference, not re-send).
        assert broker.get_actor("myapp.tasks.send_email").sent == []

    @pytest.mark.asyncio
    async def test_bulk_without_dlq_fails_closed_per_id(self, broker):
        # No DLQ holds the messages -> each id fails closed. The action
        # never re-runs an actor with an empty/guessed payload.
        result = await bulk_retry_action(
            broker,
            filter={"task_ids": ["msg-1", "msg-2"]},
            max=10,
        )
        assert result.status == "success"  # batch summary envelope
        assert result.result["retried"] == 0
        assert set(result.result["errors"]) == {"msg-1", "msg-2"}
        assert broker.get_actor("myapp.tasks.send_email").sent == []

    @pytest.mark.asyncio
    async def test_bulk_ignores_client_executable_fields_cx_h5(self, broker):
        # CX-H5 regression: even if a client smuggles actors/args/kwargs/
        # queues into the filter, the adapter IGNORES them and never
        # invokes the named actor -- it requeues by reference only. (The
        # brain also strips these; this is the adapter's defence in depth.)
        dl = _enable_dlq(broker, ["msg-1"])
        result = await bulk_retry_action(
            broker,
            filter={
                "task_ids": ["msg-1"],
                "actors": {"msg-1": "myapp.tasks.send_email"},
                "args": {"msg-1": ["attacker-payload"]},
                "kwargs": {"msg-1": {"to": "attacker@evil.example"}},
                "queues": {"msg-1": "urgent"},
            },
            max=10,
        )
        assert result.status == "success"
        assert dl.resurrected == ["msg-1"]  # requeued by reference
        # The client-named actor was NEVER sent.
        assert broker.get_actor("myapp.tasks.send_email").sent == []

    @pytest.mark.asyncio
    async def test_capped_flag_set_when_exceeds_absolute_max(self, broker):
        ids = [f"m-{i}" for i in range(20_000)]
        result = await bulk_retry_action(
            broker,
            filter={"task_ids": ids},
            max=20_000,
        )
        assert result.result["capped"] is True

    @pytest.mark.asyncio
    async def test_bulk_circuit_breaks_on_consecutive_broker_timeouts(self, broker, monkeypatch):
        # M10: a hung broker makes each DLQ resurrect offload time out, tagged
        # result["indeterminate"]. After 3 CONSECUTIVE such timeouts the batch
        # aborts rather than grinding through every id inline on the receive
        # loop; the remainder are reported skipped + circuit_broken.
        import z4j_dramatiq.actions.bulk_retry as bulk_mod
        from z4j_core.models import CommandResult

        calls = 0

        async def _hung(_broker, *, task_id, **_kwargs):
            nonlocal calls
            calls += 1
            return CommandResult(
                status="failed",
                error="broker timeout",
                result={"indeterminate": True},
            )

        monkeypatch.setattr(bulk_mod, "retry_task_action", _hung)

        ids = [f"m-{i}" for i in range(50)]
        result = await bulk_retry_action(broker, filter={"task_ids": ids}, max=100)

        assert result.status == "success"
        assert result.result["circuit_broken"] is True
        # Aborted after the 3rd consecutive timeout -- did NOT call all 50.
        assert calls == 3
        assert result.result["retried"] == 0
        assert result.result["skipped"] == len(ids) - 3

    @pytest.mark.asyncio
    async def test_bulk_intermittent_timeouts_do_not_trip_breaker(self, broker, monkeypatch):
        # A timeout that is not part of a CONSECUTIVE run resets the counter,
        # so an intermittently-slow broker still processes the whole batch and
        # never reports circuit_broken.
        import z4j_dramatiq.actions.bulk_retry as bulk_mod
        from z4j_core.models import CommandResult

        calls = 0

        async def _flaky(_broker, *, task_id, **_kwargs):
            nonlocal calls
            calls += 1
            # Every other id "times out"; never 3 in a row.
            if calls % 2 == 1:
                return CommandResult(
                    status="failed",
                    error="broker timeout",
                    result={"indeterminate": True},
                )
            return CommandResult(status="success", result={"task_id": f"new-{task_id}"})

        monkeypatch.setattr(bulk_mod, "retry_task_action", _flaky)

        ids = [f"m-{i}" for i in range(20)]
        result = await bulk_retry_action(broker, filter={"task_ids": ids}, max=100)

        assert result.status == "success"
        assert result.result["circuit_broken"] is False
        assert calls == len(ids)  # whole batch processed, no early abort
        assert result.result["skipped"] == 0


class TestDlqRequeue:
    @pytest.mark.asyncio
    async def test_fallback_when_no_native_dlq(self, broker):
        # The FakeBroker has no ``get_dead_letter`` method - the
        # action falls back to generic retry.
        from z4j_dramatiq.actions.dlq import requeue_dead_letter_action

        result = await requeue_dead_letter_action(
            broker,
            task_id="msg-1",
            actor_name="myapp.tasks.send_email",
            override_args=("hello",),
        )
        assert result.status == "success"
        assert result.result["source"] == "dlq_fallback"

    @pytest.mark.asyncio
    async def test_missing_actor_name_fails_loudly(self, broker):
        result = await requeue_dead_letter_action(
            broker,
            task_id="msg-1",
        )
        assert result.status == "failed"
        assert "actor_name" in result.error


class TestCapabilityPromotion:
    def test_bulk_retry_and_dlq_in_default(self):
        from z4j_dramatiq.capabilities import DEFAULT_CAPABILITIES

        assert "bulk_retry" in DEFAULT_CAPABILITIES
        assert "requeue_dead_letter" in DEFAULT_CAPABILITIES


def test_is_dead_letter_middleware_accepts_subclass_dramatiq_473() -> None:
    # dramatiq:473: a compatible SUBCLASS / proxy of DeadLetter (with the full
    # interface) must be accepted, not only the exact leaf class name.
    from z4j_dramatiq.actions.dlq import is_dead_letter_middleware

    class CustomDeadLetter(DeadLetter):  # subclass of the test stub named DeadLetter
        pass

    class Unrelated:
        def resurrect(self, task_id: str) -> None: ...

    class DeadLetterNoResurrect(DeadLetter):
        resurrect = None  # type: ignore[assignment]

    assert is_dead_letter_middleware(DeadLetter()) is True
    assert is_dead_letter_middleware(CustomDeadLetter()) is True  # subclass accepted
    assert is_dead_letter_middleware(Unrelated()) is False  # wrong lineage
    # DeadLetter lineage but no callable resurrect -> rejected (interface required).
    assert is_dead_letter_middleware(DeadLetterNoResurrect()) is False


class TestDlqResurrectNoopFailsClosedRL1:
    """RL1: a resurrect that returns None AND leaves the id STILL in the DLQ was
    a no-op -- the action must NOT claim success. The happy-path stub removes the
    id, so this stuck resurrect is what exercises the fail-closed branch."""

    @pytest.mark.asyncio
    async def test_requeue_noop_resurrect_fails_closed(self, broker):
        held = {"msg-1"}
        # Use a real DeadLetter (type name must be "DeadLetter" for the
        # middleware match) but override resurrect to a NO-OP that returns None
        # and does NOT remove the message -> get_dead_letter still returns it.
        dl = DeadLetter(held)

        def _stuck_resurrect(task_id: str) -> None:
            dl.resurrected.append(task_id)  # attempted, but the message stays

        dl.resurrect = _stuck_resurrect  # type: ignore[method-assign]
        broker.middleware.append(dl)
        broker.get_dead_letter = lambda tid: [tid] if tid in held else []
        # No actor_name / overrides -> after the no-op native resurrect the
        # action falls through and fails closed instead of falsely reporting a
        # by-reference success.
        result = await requeue_dead_letter_action(broker, task_id="msg-1")
        assert result.status == "failed"
        assert dl.resurrected == ["msg-1"]  # it DID attempt the resurrect
        assert "msg-1" in broker.get_dead_letter("msg-1")  # still stuck in the DLQ
