"""Fail-closed bulk retry action for Dramatiq."""

from __future__ import annotations

from typing import Any

from z4j_core.models import CommandResult


async def bulk_retry_action(
    broker: Any,
    *,
    filter: dict[str, Any] | None = None,  # noqa: A002
    max: int = 1000,  # noqa: A002
) -> CommandResult:
    """Refuse bulk retry without a recoverable completed-message store."""
    del broker, filter, max
    return CommandResult(
        status="failed",
        error=(
            "bulk_retry is unavailable for Dramatiq: stock Dramatiq has no "
            "recoverable dead-letter API or portable completed-message store"
        ),
    )


__all__ = ["bulk_retry_action"]
