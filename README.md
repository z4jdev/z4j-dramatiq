# z4j-dramatiq

[![PyPI version](https://img.shields.io/pypi/v/z4j-dramatiq.svg?v=1.4.0)](https://pypi.org/project/z4j-dramatiq/)
[![Python](https://img.shields.io/pypi/pyversions/z4j-dramatiq.svg?v=1.4.0)](https://pypi.org/project/z4j-dramatiq/)
[![License](https://img.shields.io/pypi/l/z4j-dramatiq.svg?v=1.4.0)](https://github.com/z4jdev/z4j-dramatiq/blob/main/LICENSE)

The Dramatiq engine adapter for [z4j](https://z4j.com).

Streams every Dramatiq actor lifecycle event from your workers to the
z4j and accepts operator control actions from the dashboard.
Dramatiq has no upstream scheduler, so for periodic schedules pair with
[`z4j-scheduler`](https://github.com/z4jdev/z4j-scheduler).

## What it ships

| Capability | Notes |
|---|---|
| Message lifecycle events | enqueued, started, succeeded, failed, retried, skipped |
| Actor discovery | runtime registry merge + static scan |
| Submit / retry / cancel | direct against the Dramatiq broker |
| Bulk retry | filter-driven; re-enqueues matching messages |
| Purge queue | with confirm-token guard |
| Reconcile task | via Redis / RabbitMQ broker introspection |

Captured via Dramatiq's middleware hook system, your existing actors
do not need to be decorated or modified.

## Install

```bash
pip install z4j-dramatiq
```

Pair with a framework adapter:

```bash
pip install z4j-django  z4j-dramatiq   # Django
pip install z4j-flask   z4j-dramatiq   # Flask
pip install z4j-fastapi z4j-dramatiq   # FastAPI
pip install z4j-bare    z4j-dramatiq   # framework-free worker
```

For schedules, install [`z4j-scheduler`](https://github.com/z4jdev/z4j-scheduler) as a separate process.

## Reliability

- No exception from the adapter ever propagates back into Dramatiq
  middleware or your actor code.
- Events buffer locally when z4j is unreachable; workers never
  block on network I/O.

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
