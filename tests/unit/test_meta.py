"""Tests for the ``@z4j_meta`` decorator (Dramatiq variant)."""

from __future__ import annotations

import pytest

from z4j_dramatiq.meta import META_ATTR, TaskMeta, get_meta, z4j_meta


def test_decorator_attaches_meta():
    @z4j_meta(tags=["billing"])
    def fn() -> None:
        ...
    assert isinstance(getattr(fn, META_ATTR), TaskMeta)


def test_decorator_is_noop_at_call_time():
    @z4j_meta(redact_kwargs=["x"])
    def fn(n: int) -> int:
        return n + 1
    assert fn(1) == 2


def test_get_meta_returns_attached_meta():
    @z4j_meta(deadline_ms=500)
    def fn() -> None:
        ...
    assert get_meta(fn).deadline_ms == 500


def test_get_meta_returns_none_when_missing():
    def plain() -> None:
        ...
    assert get_meta(plain) is None


def test_get_meta_walks_actor_fn_proxy():
    """Dramatiq Actor proxies its function as ``actor.fn`` - we must check both."""
    @z4j_meta(tags=["x"])
    def underlying() -> None:
        ...

    class _FakeActor:
        fn = underlying
    assert get_meta(_FakeActor()).tags == ("x",)


def test_invalid_priority_raises():
    with pytest.raises(ValueError):
        z4j_meta(priority="ultra")


def test_invalid_sample_rate_raises():
    with pytest.raises(ValueError):
        z4j_meta(sample_rate=1.5)
