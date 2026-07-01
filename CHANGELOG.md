# Changelog

## 0.0.6

### Changed
- Workflow reassignment is now load-aware. Executors report their active workflow
  count in each heartbeat, and the monitor reassigns workflows (and re-homes orphaned
  workflows) to the least-loaded healthy peer of the matching executor type instead of
  a random one. In-cycle projection prevents overloading a single peer before its next
  heartbeat refreshes its reported count.

### Added
- `active_workflow_count` is included in the heartbeat payload, persisted on the
  monitor, and exposed on the `/executors` endpoint.

### Compatibility
- Backward compatible: older executors that omit `active_workflow_count` are accepted
  (treated as `0`), and existing monitor databases migrate the new column in place on
  startup.
