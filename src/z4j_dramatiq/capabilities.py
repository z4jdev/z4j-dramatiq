"""Capability tokens advertised by the Dramatiq engine adapter.

Dramatiq's surface sits between Celery (full remote-control + pool
ops) and RQ (no remote control at all). In v2026.5 we now ship
every data-plane action the engine can support - retry, cancel
(Abortable-gated), purge, bulk_retry, requeue_dead_letter.

See `docs/MULTI_ENGINE_PLAN.md` §5 for the per-engine matrix.

Note on ``cancel_task``:
  Dramatiq supports cancel **only** when the user has the built-in
  ``Abortable`` middleware installed. The engine adapter inspects
  the broker's middleware stack and adds/removes ``cancel_task``
  from the *runtime* capability set in :meth:`capabilities`.
  ``DEFAULT_CAPABILITIES`` is the lower bound (no cancel); the
  engine adapter promotes the set when Abortable is present.
"""

from __future__ import annotations

# Lower-bound - what every Dramatiq install gets, with no
# middleware contortions required from the user.
DEFAULT_CAPABILITIES: frozenset[str] = frozenset(
    {
        "submit_task",
        "retry_task",
        "purge_queue",
        # v2026.5 promotions - round-2 landed these together with
        # the RQ promotions so the two adapters reach GA with the
        # same data-plane surface (minus engine-constraint gaps).
        "bulk_retry",
        "requeue_dead_letter",
    },
)

# Upgraded set when ``dramatiq.middleware.Abortable`` is installed
# in the user's broker stack. The engine adapter promotes to this
# set in :meth:`DramatiqEngineAdapter.capabilities`.
ABORTABLE_CAPABILITIES: frozenset[str] = DEFAULT_CAPABILITIES | {
    "cancel_task",
}


__all__ = ["ABORTABLE_CAPABILITIES", "DEFAULT_CAPABILITIES"]
