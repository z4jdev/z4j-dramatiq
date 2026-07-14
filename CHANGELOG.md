# Changelog

## 1.7.0 (2026-07-07)

* Cancel now uses the `dramatiq-abort` middleware (install with `pip install z4j-dramatiq[abort]`), guarded by a capability check so cancel is only advertised when the broker supports it.
* Python 3.11 is now the minimum supported version (3.10 dropped).
* Part of the coordinated 1.7.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.4.0 (2026-05-02)

Initial 1.4.0 release: Dramatiq engine adapter. Middleware-based capture, no decorator changes to your actors.
