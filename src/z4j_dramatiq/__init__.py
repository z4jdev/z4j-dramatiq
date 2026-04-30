"""z4j-dramatiq - Dramatiq queue engine adapter for z4j.

Public API:

- :class:`DramatiqEngineAdapter` - the adapter to pass to
  :func:`z4j_bare.install_agent`.
- :class:`Z4JMiddleware` - Dramatiq middleware that the adapter
  installs into the user's broker for event capture.
- :func:`z4j_meta` - optional per-task metadata decorator.
- :class:`TaskMeta` - normalized per-task metadata struct.

Licensed under Apache License 2.0.
"""

from __future__ import annotations

from z4j_dramatiq.engine import DramatiqEngineAdapter
from z4j_dramatiq.events.middleware import Z4JMiddleware
from z4j_dramatiq.meta import TaskMeta, z4j_meta

__version__ = "1.3.0"

__all__ = [
    "DramatiqEngineAdapter",
    "TaskMeta",
    "Z4JMiddleware",
    "__version__",
    "z4j_meta",
]
