# z4j-dramatiq

[![PyPI version](https://img.shields.io/pypi/v/z4j-dramatiq.svg)](https://pypi.org/project/z4j-dramatiq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-dramatiq.svg)](https://pypi.org/project/z4j-dramatiq/)
[![License](https://img.shields.io/pypi/l/z4j-dramatiq.svg)](https://github.com/z4jdev/z4j-dramatiq/blob/main/LICENSE)

The Dramatiq engine adapter for [z4j](https://z4j.com).

Streams supported Dramatiq actor lifecycle events from your workers to the
z4j and accepts operator control actions from the dashboard.
Dramatiq has no upstream scheduler, so for periodic schedules pair with
[`z4j-scheduler`](https://github.com/z4jdev/z4j-scheduler).

## Compatibility

- Dramatiq 1.14+ and <3 (capped below the eventual Dramatiq 3 breaking-major)
- Python 3.11+

Full per-adapter matrix at <https://z4j.dev/reference/compatibility/>.

## What it ships

| Capability | Notes |
|---|---|
| Message lifecycle events | received, started, succeeded, retried, failed |
| Actor discovery | Dramatiq's runtime broker registry |
| Submit | direct against the Dramatiq broker |
| Retry | only with complete operator-supplied replacement args and kwargs |
| Cancel pending task | requires `z4j-dramatiq[abort]` and Abortable middleware; does not interrupt running work |
| Purge queue | with confirm-token guard |
| Reconcile task | reports honest `unknown` (Dramatiq has no queryable-by-id result store); the brain's event-derived state stays authoritative |

Captured via Dramatiq's middleware hook system, your existing actors
do not need to be decorated or modified.

## Install

```bash
pip install z4j-dramatiq
```

Bulk retry and dead-letter replay are not advertised because stock Dramatiq
does not expose recoverable completed-message or dead-letter APIs. Pending-task
cancel is advertised only when the external `dramatiq-abort` Abortable
middleware is installed. z4j uses its pending-only mode because interrupting a
running task can interact with Dramatiq retries and enqueue another attempt.

Pair with a framework adapter:

```bash
pip install z4j-django  z4j-dramatiq   # Django
pip install z4j-flask   z4j-dramatiq   # Flask
pip install z4j-fastapi z4j-dramatiq   # FastAPI
pip install z4j-bare    z4j-dramatiq   # framework-free worker
```

For schedules, install [`z4j-scheduler`](https://github.com/z4jdev/z4j-scheduler) as a separate process.

## Reliability

- Lifecycle-capture failures are isolated from Dramatiq middleware and actor
  code; capture hooks make no brain network request inline.
- The in-process event queue and SQLite outbound buffer are bounded. Queue
  overflow drops new events and buffer pressure evicts oldest rows; both losses
  are logged.

## Documentation

Full docs at [z4j.dev/engines/dramatiq/](https://z4j.dev/engines/dramatiq/).

## License

Apache-2.0, see [LICENSE](LICENSE).

## Links

- Homepage: https://z4j.com
- Documentation: https://z4j.dev
- PyPI: https://pypi.org/project/z4j-dramatiq/
- Issues: https://github.com/z4jdev/z4j-dramatiq/issues
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Security: security@z4j.com (see [SECURITY.md](SECURITY.md))
