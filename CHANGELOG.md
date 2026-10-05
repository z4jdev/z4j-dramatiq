# Changelog

## 1.12.1 (2026-10-05)

* The requirement is `dramatiq>=1.14` with no upper bound, in the base
  dependency and in the `redis` and `rabbitmq` extras. No code change.

## 1.12.0 (2026-10-03)

* Advertise `list_dead_letters` where the broker's dead-letter store can be
  read without consuming it. On Redis the `dlq.list` command lists the
  `<queue>.XQ` store with full entries (id, actor, dead-letter time,
  attempts, a redacted traceback excerpt); bodies are decoded as JSON only,
  so a `PickleEncoder` deployment gets id, queue and time with an empty task
  name and excerpt. On RabbitMQ the page carries `total` only, the `.XQ`
  message count, because AMQP has no non-destructive read of queued
  messages. `requeue_dead_letter` stays absent: Dramatiq has no
  resurrect-by-id API.

## 1.11.0 (2026-09-10)

* Align runtime version metadata and sibling dependency floors with the coordinated 1.11.0 release.

## 1.10.0 (2026-08-28)

* Carried with the coordinated fleet release. No behaviour changed.

## 1.9.1 (2026-08-27)

* Carried with the coordinated fleet release. No adapter behaviour changed.

## 1.9.0 (2026-08-25)

* Purge now reports the complete ready, delayed, and dead message count returned
  by Redis and RabbitMQ broker APIs.
* Retry is available only when the operator supplies both complete replacement
  argument collections. Stock Dramatiq has no recoverable completed-message or
  dead-letter API, so a no-override retry fails closed.
* Bulk retry and dead-letter replay are no longer advertised. The earlier
  implementations relied on dead-letter interfaces stock Dramatiq does not
  provide; direct calls now fail without publishing work.
* With `dramatiq-abort` and its Abortable middleware installed, cancel retains
  pending-task cancellation. It deliberately does not interrupt already-running
  work because Dramatiq's Retries middleware can enqueue another attempt.
* Retry lifecycle events are classified after Dramatiq's Retries middleware has
  decided whether another attempt was enqueued.

## 1.8.0 (2026-07-23)

* Its safe retry attestation is now carried on the exact executing session, and package floors require the coordinated 1.8.0 bare/core runtime.
* Retry stopped the unsafe empty-payload fallback. The release still incorrectly
  advertised bulk retry and dead-letter replay through an interface stock
  Dramatiq does not provide; those claims and capabilities are withdrawn above.
* Purge broker I/O is offloaded. The claimed bulk-retry circuit breaker and
  native dead-letter resurrection path were unreachable and are withdrawn above.
* Part of the coordinated 1.8.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.7.0 (2026-07-07)

* Cancel through `dramatiq-abort` is limited to pending work; the running-abort
  mode is not used because its exception interacts with Dramatiq retries.
* Python 3.11 is now the minimum supported version (3.10 dropped).
* Part of the coordinated 1.7.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.4.0 (2026-05-02)

Initial 1.4.0 release: Dramatiq engine adapter. Middleware-based capture, no decorator changes to your actors.
