"""Capability tokens advertised by the Dramatiq engine adapter.

Dramatiq's portable action surface is task submission, retry with complete
operator-supplied replacement inputs, and guarded queue purge.

See `docs/MULTI_ENGINE_PLAN.md` §5 for the per-engine matrix.

The separate ``dramatiq-abort`` package can revoke pending work. z4j advertises
cancel only when its ``Abortable`` middleware is present and deliberately uses
pending-only mode: running abort interacts unsafely with Dramatiq retries.
Stock Dramatiq has no recoverable dead-letter API.
"""

from __future__ import annotations

# Lower-bound - what every Dramatiq install gets, with no
# middleware contortions required from the user.
DEFAULT_CAPABILITIES: frozenset[str] = frozenset(
    {
        "submit_task",
        "retry_task",
        "purge_queue",
    },
)

# Promoted only when the external Abortable middleware is present. The action
# uses dramatiq-abort's pending-only mode and does not claim to interrupt work
# that is already running.
ABORTABLE_CAPABILITIES: frozenset[str] = DEFAULT_CAPABILITIES | {"cancel_task"}


__all__ = ["ABORTABLE_CAPABILITIES", "DEFAULT_CAPABILITIES"]
