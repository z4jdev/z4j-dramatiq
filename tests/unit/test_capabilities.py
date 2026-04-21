"""Capability set freeze for the Dramatiq adapter."""

from __future__ import annotations

from z4j_dramatiq.capabilities import (
    ABORTABLE_CAPABILITIES,
    DEFAULT_CAPABILITIES,
)


def test_default_set_frozen():
    # v2026.5 GA set - see docs/MULTI_ENGINE_PLAN.md §5.
    assert DEFAULT_CAPABILITIES == frozenset(
        {
            "submit_task",
            "retry_task",
            "purge_queue",
            "bulk_retry",
            "requeue_dead_letter",
        },
    )


def test_abortable_set_includes_default_plus_cancel():
    assert ABORTABLE_CAPABILITIES == DEFAULT_CAPABILITIES | {"cancel_task"}


def test_no_remote_control_in_either_set():
    for absent in ("restart_worker", "rate_limit"):
        assert absent not in DEFAULT_CAPABILITIES
        assert absent not in ABORTABLE_CAPABILITIES


def test_both_sets_are_frozen():
    assert isinstance(DEFAULT_CAPABILITIES, frozenset)
    assert isinstance(ABORTABLE_CAPABILITIES, frozenset)
