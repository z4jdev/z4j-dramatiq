"""Discover the user's Dramatiq actor surface.

Dramatiq has a real registry - the broker's ``actors`` dict - so
discovery is much cleaner than RQ's. Walk the dict, emit one
:class:`TaskDefinition` per registered actor.
"""

from __future__ import annotations

import logging
from typing import Any

from z4j_core.models import TaskDefinition

from z4j_dramatiq.events.mapper import DRAMATIQ_ENGINE_NAME

logger = logging.getLogger("z4j.agent.dramatiq.discovery")


def discover_runtime(broker: Any) -> list[TaskDefinition]:
    """Return one ``TaskDefinition`` per actor registered on ``broker``.

    ``broker`` is duck-typed: any object with an ``actors`` mapping
    (the public attribute every Dramatiq Broker exposes) works.
    """
    actors = getattr(broker, "actors", None)
    if not actors:
        return []

    out: list[TaskDefinition] = []
    try:
        items = list(actors.items())
    except Exception:  # noqa: BLE001
        return []

    for name, actor in items:
        try:
            queue = _safe_str(getattr(actor, "queue_name", "default"))
            module = _module_for(actor)
            out.append(
                TaskDefinition(
                    name=_safe_str(name),
                    engine=DRAMATIQ_ENGINE_NAME,
                    queue=queue or "default",
                    module=module,
                ),
            )
        except Exception:  # noqa: BLE001
            # Single bad actor must not block discovery of the rest.
            logger.exception(
                "z4j dramatiq: discovery failed for actor %r", name,
            )
    return out


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    try:
        return str(value)
    except Exception:  # noqa: BLE001
        return ""


def _module_for(actor: Any) -> str:
    """Pull the module name off the underlying function, best-effort."""
    fn = getattr(actor, "fn", None) or actor
    module = getattr(fn, "__module__", "")
    return _safe_str(module)


__all__ = ["discover_runtime"]
