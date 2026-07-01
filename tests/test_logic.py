"""Unit tests for core logic that don't require Postgres."""

from unittest.mock import AsyncMock

from dbos_monitor.service.config import MonitorConfig
from dbos_monitor.service.monitor_db import ExecutorRecord
from dbos_monitor.service.scheduler import (
	_pick_least_loaded,
	_run_orphan_cycle,
	_run_reassignment_cycle,
)


def _config(**overrides):
	base = dict(
		executor_health_ping_grace_timeout_ms=5000,
		unknown_executor_health_ping_timeout_ms=30000,
		dbos_postgres_connection_uri="postgresql://x",
		monitor_postgres_connection_uri="postgresql://x",
	)
	base.update(overrides)
	return MonitorConfig(**base)


def _executor(executor_id, executor_type, healthy=True, active_workflow_count=0):
	return {
		"executor_id": executor_id,
		"executor_type": executor_type,
		"healthy": healthy,
		"active_workflow_count": active_workflow_count,
	}


def _record(executor_id, executor_type="worker", active_workflow_count=0):
	return ExecutorRecord(executor_id, executor_type, 1000, 0, 0, False, active_workflow_count)


def test_pick_least_loaded_returns_lowest_load():
	candidates = ["a", "b", "c"]
	loads = {"a": 5, "b": 2, "c": 9}
	assert _pick_least_loaded(candidates, lambda c: loads[c]) == "b"


def test_pick_least_loaded_tie_breaks_within_tied_set():
	candidates = ["a", "b", "c"]
	loads = {"a": 2, "b": 2, "c": 9}
	# Either tied candidate is acceptable, never the heavier one.
	assert _pick_least_loaded(candidates, lambda c: loads[c]) in {"a", "b"}


async def test_reassignment_cycle_skips_when_no_healthy_targets():
	config = MonitorConfig(
		executor_health_ping_grace_timeout_ms=5000,
		unknown_executor_health_ping_timeout_ms=30000,
		dbos_postgres_connection_uri="postgresql://x",
		monitor_postgres_connection_uri="postgresql://x",
	)

	unhealthy = [_record("dead-exec")]

	monitor_db = AsyncMock()
	monitor_db.get_unhealthy_executors.return_value = unhealthy
	monitor_db.get_healthy_executors_by_type.return_value = []

	dbos_db = AsyncMock()

	await _run_reassignment_cycle(config, monitor_db, dbos_db)

	dbos_db.reassign_workflows.assert_not_called()
	monitor_db.mark_recovery_needed.assert_not_called()


async def test_reassignment_cycle_reassigns_to_healthy_target():
	config = MonitorConfig(
		executor_health_ping_grace_timeout_ms=5000,
		unknown_executor_health_ping_timeout_ms=30000,
		dbos_postgres_connection_uri="postgresql://x",
		monitor_postgres_connection_uri="postgresql://x",
	)

	unhealthy = [_record("dead-exec")]
	healthy = [_record("alive-exec")]

	monitor_db = AsyncMock()
	monitor_db.get_unhealthy_executors.return_value = unhealthy
	monitor_db.get_healthy_executors_by_type.return_value = healthy

	dbos_db = AsyncMock()
	dbos_db.reassign_workflows.return_value = ["wf-1", "wf-2"]

	await _run_reassignment_cycle(config, monitor_db, dbos_db)

	dbos_db.reassign_workflows.assert_called_once_with(
		old_executor_id="dead-exec",
		new_executor_id="alive-exec",
		age_threshold_ms=None,
		max_batch_size=20,
	)
	monitor_db.mark_recovery_needed.assert_called_once_with("alive-exec")
	monitor_db.remove_executor.assert_called_once_with("dead-exec")


async def test_reassignment_cycle_removes_executor_even_with_no_workflows():
	config = MonitorConfig(
		executor_health_ping_grace_timeout_ms=5000,
		unknown_executor_health_ping_timeout_ms=30000,
		dbos_postgres_connection_uri="postgresql://x",
		monitor_postgres_connection_uri="postgresql://x",
	)

	unhealthy = [_record("dead-exec")]
	healthy = [_record("alive-exec")]

	monitor_db = AsyncMock()
	monitor_db.get_unhealthy_executors.return_value = unhealthy
	monitor_db.get_healthy_executors_by_type.return_value = healthy

	dbos_db = AsyncMock()
	dbos_db.reassign_workflows.return_value = []

	await _run_reassignment_cycle(config, monitor_db, dbos_db)

	monitor_db.mark_recovery_needed.assert_not_called()
	monitor_db.remove_executor.assert_called_once_with("dead-exec")


async def test_reassignment_cycle_drains_in_batches_within_one_call():
	"""A full batch means more may remain, so keep draining (back-to-back) until a short
	batch arrives; only then is the executor removed."""
	config = MonitorConfig(
		executor_health_ping_grace_timeout_ms=5000,
		unknown_executor_health_ping_timeout_ms=30000,
		dbos_postgres_connection_uri="postgresql://x",
		monitor_postgres_connection_uri="postgresql://x",
		reassignment_max_batch_size=2,
	)

	unhealthy = [_record("dead-exec")]
	healthy = [_record("alive-exec")]

	monitor_db = AsyncMock()
	monitor_db.get_unhealthy_executors.return_value = unhealthy
	monitor_db.get_healthy_executors_by_type.return_value = healthy

	dbos_db = AsyncMock()
	# First batch is full (2) -> keep going; second batch is short (1) -> drained.
	dbos_db.reassign_workflows.side_effect = [["wf-1", "wf-2"], ["wf-3"]]

	await _run_reassignment_cycle(config, monitor_db, dbos_db)

	assert dbos_db.reassign_workflows.call_count == 2
	assert monitor_db.mark_recovery_needed.call_count == 2
	monitor_db.remove_executor.assert_called_once_with("dead-exec")


async def test_reassignment_prefers_least_loaded_then_shifts_as_load_projects():
	"""The lighter peer is drained into first; once the workflows assigned this cycle push its
	projected load past the heavier peer, later batches shift to the heavier peer."""
	config = _config(reassignment_max_batch_size=2)

	monitor_db = AsyncMock()
	monitor_db.get_unhealthy_executors.return_value = [_record("dead-exec")]
	monitor_db.get_healthy_executors_by_type.return_value = [
		_record("light", active_workflow_count=0),
		_record("heavy", active_workflow_count=3),
	]

	dbos_db = AsyncMock()
	# Two full batches (keep draining) then a short one (drained).
	dbos_db.reassign_workflows.side_effect = [["a", "b"], ["c", "d"], ["e"]]

	await _run_reassignment_cycle(config, monitor_db, dbos_db)

	targets = [c.kwargs["new_executor_id"] for c in dbos_db.reassign_workflows.call_args_list]
	# light: 0 -> picked, +2 -> picked again (2 < 3), +2 -> now 4 > 3 so heavy takes the last batch.
	assert targets == ["light", "light", "heavy"]


async def test_orphan_cycle_routes_to_least_loaded_peer():
	"""With a clear load gap, all orphans of a type go to the lighter peer."""
	monitor_db = AsyncMock()
	monitor_db.get_all_executor_ids.return_value = ["light", "heavy"]
	monitor_db.get_types_for_workflows.return_value = {"ingest": "worker"}
	monitor_db.get_all_executors.return_value = [
		_executor("light", "worker", active_workflow_count=0),
		_executor("heavy", "worker", active_workflow_count=10),
	]

	dbos_db = AsyncMock()
	dbos_db.get_pending_workflows_not_owned_by.return_value = [
		{"workflow_uuid": f"wf-{i}", "name": "ingest", "executor_id": "ghost"} for i in range(3)
	]
	dbos_db.reassign_workflow_ids.side_effect = lambda uuids, target: uuids

	await _run_orphan_cycle(_config(), monitor_db, dbos_db)

	dbos_db.reassign_workflow_ids.assert_called_once_with(["wf-0", "wf-1", "wf-2"], "light")


def test_config_from_env(monkeypatch):
	monkeypatch.setenv("DBOS_MONITOR_DBOS_POSTGRES_CONNECTION_URI", "postgresql://dbos")
	monkeypatch.setenv("DBOS_MONITOR_MONITOR_POSTGRES_CONNECTION_URI", "postgresql://monitor")
	monkeypatch.setenv("DBOS_MONITOR_EXECUTOR_HEALTH_PING_GRACE_TIMEOUT_MS", "10000")
	monkeypatch.setenv("DBOS_MONITOR_LOG_LEVEL", "DEBUG")

	config = MonitorConfig()
	assert config.dbos_postgres_connection_uri == "postgresql://dbos"
	assert config.monitor_postgres_connection_uri == "postgresql://monitor"
	assert config.executor_health_ping_grace_timeout_ms == 10000
	assert config.log_level == "DEBUG"


# --- Orphan assignment cycle ---


async def test_orphan_cycle_skips_when_no_known_executors():
	"""Guard: with zero known executors, every PENDING workflow would look abandoned."""
	monitor_db = AsyncMock()
	monitor_db.get_all_executor_ids.return_value = []

	dbos_db = AsyncMock()

	await _run_orphan_cycle(_config(), monitor_db, dbos_db)

	dbos_db.get_pending_workflows_not_owned_by.assert_not_called()


async def test_orphan_cycle_reassigns_to_compatible_target():
	monitor_db = AsyncMock()
	monitor_db.get_all_executor_ids.return_value = ["worker-1"]
	monitor_db.get_types_for_workflows.return_value = {"ingest": "worker"}
	monitor_db.get_all_executors.return_value = [_executor("worker-1", "worker")]

	dbos_db = AsyncMock()
	dbos_db.get_pending_workflows_not_owned_by.return_value = [
		{"workflow_uuid": "wf-1", "name": "ingest", "executor_id": "ghost"},
	]
	dbos_db.reassign_workflow_ids.return_value = ["wf-1"]

	await _run_orphan_cycle(_config(), monitor_db, dbos_db)

	dbos_db.reassign_workflow_ids.assert_called_once_with(["wf-1"], "worker-1")
	monitor_db.mark_recovery_needed.assert_called_once_with("worker-1")


async def test_orphan_cycle_picks_one_of_several_healthy_peers():
	"""Several healthy executors of the mapped type -> the orphan goes to one of them."""
	monitor_db = AsyncMock()
	monitor_db.get_all_executor_ids.return_value = ["worker-1", "worker-2"]
	monitor_db.get_types_for_workflows.return_value = {"ingest": "worker"}
	monitor_db.get_all_executors.return_value = [
		_executor("worker-1", "worker"),
		_executor("worker-2", "worker"),
	]

	dbos_db = AsyncMock()
	dbos_db.get_pending_workflows_not_owned_by.return_value = [
		{"workflow_uuid": "wf-1", "name": "ingest", "executor_id": "ghost"},
	]
	dbos_db.reassign_workflow_ids.return_value = ["wf-1"]

	await _run_orphan_cycle(_config(), monitor_db, dbos_db)

	(uuids, target), _ = dbos_db.reassign_workflow_ids.call_args
	assert uuids == ["wf-1"]
	assert target in {"worker-1", "worker-2"}
	monitor_db.mark_recovery_needed.assert_called_once_with(target)


async def test_orphan_cycle_leaves_unplaceable_workflows():
	"""No inferred type (or no healthy executor of an inferred type) -> nothing reassigned."""
	monitor_db = AsyncMock()
	monitor_db.get_all_executor_ids.return_value = ["worker-1"]
	monitor_db.get_types_for_workflows.return_value = {}  # unknown workflow name
	monitor_db.get_all_executors.return_value = [_executor("worker-1", "worker")]

	dbos_db = AsyncMock()
	dbos_db.get_pending_workflows_not_owned_by.return_value = [
		{"workflow_uuid": "wf-1", "name": "mystery", "executor_id": "ghost"},
	]

	await _run_orphan_cycle(_config(), monitor_db, dbos_db)

	dbos_db.reassign_workflow_ids.assert_not_called()
	monitor_db.mark_recovery_needed.assert_not_called()
