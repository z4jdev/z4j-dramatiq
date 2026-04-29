"""z4j-dramatiq CLI: ``z4j-dramatiq doctor | check | status | version``."""

from __future__ import annotations

from z4j_bare.cli import make_engine_main

# Dramatiq doesn't have a single canonical broker env var (it
# supports Redis, RabbitMQ, and stub brokers), so we don't probe
# one. The framework's doctor checks the resolved broker URL via
# the dramatiq broker object instead.
main = make_engine_main(
    "dramatiq",
    upstream_package="dramatiq",
    broker_env=None,
)


__all__ = ["main"]
