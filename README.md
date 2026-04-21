# z4j-dramatiq

[![PyPI version](https://img.shields.io/pypi/v/z4j-dramatiq.svg)](https://pypi.org/project/z4j-dramatiq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-dramatiq.svg)](https://pypi.org/project/z4j-dramatiq/)
[![License](https://img.shields.io/pypi/l/z4j-dramatiq.svg)](https://github.com/z4jdev/z4j-dramatiq/blob/main/LICENSE)


**License:** Apache 2.0
**Status:** v1.0.0 - first public release alongside `z4j-celery` and `z4j-rq`.

The [Dramatiq](https://dramatiq.io/) queue-engine adapter for z4j.
Drop into any Dramatiq install (Redis OR RabbitMQ broker - both
first-class on day 1) and z4j observes every actor your workers run
via the canonical Dramatiq middleware chain - no monkey-patches, no
subclassing, no host code changes.

## Install

```bash
# Redis broker
pip install z4j[dramatiq] dramatiq[redis]
# OR RabbitMQ broker
pip install z4j[dramatiq] dramatiq[rabbitmq]

# Or just this package:
pip install z4j-dramatiq
```

## What it ships on day 1

| Capability | Status | Notes |
|---|---|---|
| Event capture (received / started / succeeded / failed) | ✅ | `Z4JMiddleware` in your broker chain - blessed observability hook |
| Discovery | ✅ | Walks `broker.actors` |
| `retry` | ✅ | Re-sends the actor's message |
| `cancel` | ⚠️ | Promoted into capabilities **iff** Abortable middleware is installed |
| `purge_queue` | ✅ | With confirm-token + Z4J_PURGE_THRESHOLD guard |
| Dual-broker (Redis + RabbitMQ) | ✅ | Both natively supported, both in CI |

## What it deliberately does NOT ship

| Capability | Why |
|---|---|
| `bulk_retry` | Deferred to v1.1 |
| `requeue_dead_letter` | Deferred to v1.1 (Dramatiq has DeadLetter middleware) |
| `rate_limit` | Deferred to v1.1 (Dramatiq has Throttler middleware) |
| `restart_worker` | Dramatiq workers expose no remote-control channel - never |
| `pool grow / shrink / consumer ops` | Dramatiq has no pool concept - never |

The adapter advertises only what it implements via
`capabilities()` - the dashboard hides every button it can't honor.
The cancel button only appears if `Abortable` is in your middleware
stack.

## Scheduler pairing

Dramatiq has no built-in scheduler; use
[`z4j-apscheduler`](https://github.com/z4jdev/z4j-apscheduler) to
surface APScheduler-driven periodic / cron jobs on the Schedules page.

## Documentation

- [Dramatiq engine guide](https://z4j.dev/engines/dramatiq/)
- [Architecture](https://z4j.dev/concepts/architecture/)
- [Adapter protocol](https://z4j.dev/concepts/adapter-axes/)

## License

Apache 2.0 - see [LICENSE](LICENSE).

## Links

- Homepage: <https://z4j.com>
- Documentation: <https://z4j.dev>
- Issues: <https://github.com/z4jdev/z4j-dramatiq/issues>
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: `security@z4j.com` (see [SECURITY.md](SECURITY.md))
