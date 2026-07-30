# Changelog

## 1.8.0 (2026-07-23)

* Its safe retry attestation is now carried on the exact executing session, and package floors require the coordinated 1.8.0 bare/core runtime.
* Retry and bulk_retry now requeue failed messages BY REFERENCE through the dead-letter queue and ignore any client-supplied actor / args / kwargs (closing a confused-deputy actor-invocation path); an id with no DLQ entry fails closed rather than re-sending an empty payload.
* Destructive actions offload their broker I/O; a consecutive-timeout circuit breaker aborts a bulk retry against a hung broker (reporting the remainder skipped); DLQ requeue threads the actor name and tries the native DLQ resurrect first.
* Part of the coordinated 1.8.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.7.0 (2026-07-07)

* Cancel now uses the `dramatiq-abort` middleware (install with `pip install z4j-dramatiq[abort]`), guarded by a capability check so cancel is only advertised when the broker supports it.
* Python 3.11 is now the minimum supported version (3.10 dropped).
* Part of the coordinated 1.7.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.4.0 (2026-05-02)

Initial 1.4.0 release: Dramatiq engine adapter. Middleware-based capture, no decorator changes to your actors.
