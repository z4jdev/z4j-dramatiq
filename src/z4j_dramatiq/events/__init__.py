"""Dramatiq event-capture surface.

Single capture path: a :class:`Z4JMiddleware` slotted into the
user's broker. Dramatiq's middleware chain is the *blessed* hook
point - `before_process_message`, `after_process_message`,
`after_enqueue` are explicitly designed for observability tools
like this one.

The mapper in :mod:`z4j_dramatiq.events.mapper` is the single
place that translates a Dramatiq ``Message`` into a
:class:`z4j_core.models.Event`.
"""

from __future__ import annotations

from z4j_dramatiq.events.mapper import build_event
from z4j_dramatiq.events.middleware import Z4JMiddleware

__all__ = ["Z4JMiddleware", "build_event"]
