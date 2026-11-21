## Unreleased

### Improvements

- Store initialization is now a recoverable transaction. The log/conf buckets,
  log entries, stable metadata and a committed-state marker (format version
  plus materialized first/last index) share one commit boundary, so a crash
  during initialization can no longer leave contradictory index ranges.
  Applies to both the v1 and v2 modules.
- Reads only observe fully committed state. Missing buckets, a partially
  written or malformed state marker, and marker/log range mismatches now
  return an identifiable `ErrCorrupt` error (detectable with `errors.Is`)
  instead of being silently ignored.
- Databases written by older releases (no state marker) are adopted on the
  first write rather than reset; existing logs and stable metadata are always
  preserved.
- Operations after `Close` now return `ErrStoreClosed` (wrapping the
  underlying `ErrDatabaseNotOpen`). `Close` is idempotent and waits for
  in-flight transactions to finish, making concurrent reads, writes and
  closes safe.
- `MigrateToV2` (v2) commits the state marker together with the migrated data.

### Changes

### Fixed

### Security
