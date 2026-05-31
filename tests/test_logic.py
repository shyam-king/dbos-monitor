"""Unit tests for core logic that don't require Postgres."""

from unittest.mock import AsyncMock

from dbos_monitor.service.config import MonitorConfig
from dbos_monitor.service.monitor_db import ExecutorRecord
from dbos_monitor.service.scheduler import _run_reassignment_cycle, _select_target


def test_select_target_picks_a_healthy_executor():
	executors = [
		ExecutorRecord("exec-1", "worker", 1000, 0, 0, False),
		ExecutorRecord("exec-2", "worker", 1000, 0, 0, False),
	]
	# Selection is random, so just assert it returns one of the candidates.
	assert _select_target(executors) in executors


async def test_reassignment_cycle_skips_when_no_healthy_targets():
	config = MonitorConfig(
		executor_health_ping_grace_timeout_ms=5000,
		unknown_executor_health_ping_timeout_ms=30000,
		dbos_postgres_connection_uri="postgresql://x",
		monitor_postgres_connection_uri="postgresql://x",
	)

	unhealthy = [ExecutorRecord("dead-exec", "worker", 1000, 0, 0, False)]

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

	unhealthy = [ExecutorRecord("dead-exec", "worker", 1000, 0, 0, False)]
	healthy = [ExecutorRecord("alive-exec", "worker", 1000, 0, 0, False)]

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

	unhealthy = [ExecutorRecord("dead-exec", "worker", 1000, 0, 0, False)]
	healthy = [ExecutorRecord("alive-exec", "worker", 1000, 0, 0, False)]

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

	unhealthy = [ExecutorRecord("dead-exec", "worker", 1000, 0, 0, False)]
	healthy = [ExecutorRecord("alive-exec", "worker", 1000, 0, 0, False)]

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
