"""The ``@z4j_meta`` decorator for Dramatiq actors.

Identical surface to :mod:`z4j_celery.meta` and :mod:`z4j_rq.meta`
so users moving between engines find one decorator that works
everywhere.

Example::

    import dramatiq
    from z4j_dramatiq import z4j_meta

    @dramatiq.actor(queue_name="emails")
    @z4j_meta(redact_kwargs=["api_key"], tags=["billing"])
    def send_invoice(user_id, api_key, amount):
        ...
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

META_ATTR = "__z4j_meta__"


@dataclass(frozen=True, slots=True)
class TaskMeta:
    """Normalized per-actor z4j metadata."""

    redact_kwargs: frozenset[str] = field(default_factory=frozenset)
    keep_kwargs: frozenset[str] | None = None
    redact_result: bool = False
    tags: tuple[str, ...] = ()
    priority: str | None = None
    expected_duration_ms: int | None = None
    deadline_ms: int | None = None
    skip: bool = False
    sample_rate: float = 1.0


def z4j_meta(
    *,
    redact_kwargs: Iterable[str] | None = None,
    keep_kwargs: Iterable[str] | None = None,
    redact_result: bool = False,
    tags: Iterable[str] | None = None,
    priority: str | None = None,
    expected_duration_ms: int | None = None,
    deadline_ms: int | None = None,
    skip: bool = False,
    sample_rate: float = 1.0,
) -> Callable[[F], F]:
    """Attach z4j metadata to a Dramatiq actor's underlying function."""
    if not 0.0 <= sample_rate <= 1.0:
        raise ValueError("sample_rate must be in [0.0, 1.0]")
    valid_priorities = {"critical", "high", "normal", "low", None}
    if priority not in valid_priorities:
        raise ValueError(
            f"priority must be one of {valid_priorities - {None}}, got {priority!r}",
        )

    meta = TaskMeta(
        redact_kwargs=frozenset(redact_kwargs or ()),
        keep_kwargs=frozenset(keep_kwargs) if keep_kwargs is not None else None,
        redact_result=redact_result,
        tags=tuple(tags or ()),
        priority=priority,
        expected_duration_ms=expected_duration_ms,
        deadline_ms=deadline_ms,
        skip=skip,
        sample_rate=sample_rate,
    )

    def decorator(func: F) -> F:
        setattr(func, META_ATTR, meta)
        return func

    return decorator


def get_meta(func: Any) -> TaskMeta | None:
    """Return ``TaskMeta`` attached to a function (or a Dramatiq Actor's ``.fn``)."""
    if func is None:
        return None
    target = getattr(func, "fn", func)  # Dramatiq Actor proxies fn
    return getattr(target, META_ATTR, None)


__all__ = ["META_ATTR", "TaskMeta", "get_meta", "z4j_meta"]
