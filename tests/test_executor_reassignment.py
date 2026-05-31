import asyncio
import time

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from dbos_monitor.service.app import create_app
from dbos_monitor.service.config import MonitorConfig
from dbos_monitor.service.dbos_db import DbosDB
from dbos_monitor.service.monitor_db import MonitorDB
from dbos_monitor.service.scheduler import _run_reassignment_cycle

pytestmark = pytest.mark.integration


@pytest.fixture
def monitor_config(postgres_url):
	return MonitorConfig(
		executor_health_ping_grace_timeout_ms=500,
		unknown_executor_health_ping_timeout_ms=2000,
		dbos_postgres_connection_uri=postgres_url,
		monitor_postgres_connection_uri=postgres_url,
		reassignment_loop_interval_ms=200,
	)


@pytest.fixture
async def running_app(monitor_config, clean_tables):
	app = create_app(monitor_config)

	monitor_db = MonitorDB(monitor_config.monitor_postgres_connection_uri)
	dbos_db = DbosDB(monitor_config.dbos_postgres_connection_uri)
	await monitor_db.connect()
	await dbos_db.connect()

	app.state.monitor_db = monitor_db
	app.state.dbos_db = dbos_db
	app.state.config = monitor_config

	transport = ASGITransport(app=app)
	async with AsyncClient(transport=transport, base_url="http://test") as client:
		yield client, monitor_config, monitor_db, dbos_db

	await monitor_db.close()
	await dbos_db.close()


async def _insert_pending_workflow(postgres_url: str, workflow_id: str, executor_id: str):
	now_ms = int(time.time() * 1000)
	async with await psycopg.AsyncConnection.connect(postgres_url, autocommit=True) as conn:
		await conn.execute(
			"""
            INSERT INTO dbos.workflow_status
                (workflow_uuid, status, name, executor_id, created_at, updated_at)
            VALUES (%(id)s, 'PENDING', 'test_workflow', %(exec)s, %(now)s, %(now)s)
            """,
			{"id": workflow_id, "exec": executor_id, "now": now_ms},
		)


async def _get_workflow_executor(postgres_url: str, workflow_id: str) -> str | None:
	async with await psycopg.AsyncConnection.connect(postgres_url, autocommit=True) as conn:
		row = await (
			await conn.execute(
				"SELECT executor_id FROM dbos.workflow_status WHERE workflow_uuid = %(id)s",
				{"id": workflow_id},
			)
		).fetchone()
		return row[0] if row else None


async def test_workflow_reassignment_on_executor_crash(running_app, postgres_url):
	"""
	Scenario:
	1. executor-a registers and has a PENDING workflow
	2. executor-a stops sending heartbeats (simulates crash)
	3. executor-b registers with the same executor_type
	4. After grace period, monitor reassigns executor-a's workflow to executor-b
	5. executor-b receives recovery_needed=true on next heartbeat
	"""
	client, config, monitor_db, dbos_db = running_app
	await _insert_pending_workflow(postgres_url, "wf-001", "executor-a")

	resp = await client.post(
		"/heartbeat",
		json={
			"executor_id": "executor-a",
			"executor_type": "worker",
			"health_ping_interval_ms": 200,
		},
	)
	assert resp.status_code == 200
	assert resp.json()["recovery_needed"] is False

	resp = await client.post(
		"/heartbeat",
		json={
			"executor_id": "executor-b",
			"executor_type": "worker",
			"health_ping_interval_ms": 5000,
		},
	)
	assert resp.status_code == 200

	# Wait for executor-a to become unhealthy (ping_interval 200ms + grace 500ms)
	await asyncio.sleep(1.0)

	# Run the reassignment cycle
	await _run_reassignment_cycle(config, monitor_db, dbos_db)

	# Verify workflow was reassigned to executor-b
	new_executor = await _get_workflow_executor(postgres_url, "wf-001")
	assert new_executor == "executor-b"

	# Verify executor-b gets recovery_needed on next heartbeat
	resp = await client.post(
		"/heartbeat",
		json={
			"executor_id": "executor-b",
			"executor_type": "worker",
			"health_ping_interval_ms": 5000,
		},
	)
	assert resp.status_code == 200
	assert resp.json()["recovery_needed"] is True

	# Subsequent heartbeat should NOT have recovery_needed (already cleared)
	resp = await client.post(
		"/heartbeat",
		json={
			"executor_id": "executor-b",
			"executor_type": "worker",
			"health_ping_interval_ms": 5000,
		},
	)
	assert resp.json()["recovery_needed"] is False


async def test_no_reassignment_across_executor_types(running_app, postgres_url):
	"""Workflows should only be reassigned to executors of the same type."""
	client, config, monitor_db, dbos_db = running_app
	await _insert_pending_workflow(postgres_url, "wf-002", "executor-x")

	# Register executor-x as type "api"
	await client.post(
		"/heartbeat",
		json={
			"executor_id": "executor-x",
			"executor_type": "api",
			"health_ping_interval_ms": 200,
		},
	)

	# Register executor-y as type "worker" (different type)
	await client.post(
		"/heartbeat",
		json={
			"executor_id": "executor-y",
			"executor_type": "worker",
			"health_ping_interval_ms": 5000,
		},
	)

	# Wait for executor-x to become unhealthy
	await asyncio.sleep(1.0)

	# Run the reassignment cycle
	await _run_reassignment_cycle(config, monitor_db, dbos_db)

	# Workflow should still belong to executor-x (no compatible target)
	current_executor = await _get_workflow_executor(postgres_url, "wf-002")
	assert current_executor == "executor-x"


async def test_multiple_workflows_reassigned(running_app, postgres_url):
	"""All PENDING workflows from a crashed executor are reassigned together."""
	client, config, monitor_db, dbos_db = running_app
	for i in range(5):
		await _insert_pending_workflow(postgres_url, f"wf-multi-{i}", "executor-c")

	await client.post(
		"/heartbeat",
		json={
			"executor_id": "executor-c",
			"executor_type": "batch",
			"health_ping_interval_ms": 200,
		},
	)
	await client.post(
		"/heartbeat",
		json={
			"executor_id": "executor-d",
			"executor_type": "batch",
			"health_ping_interval_ms": 5000,
		},
	)

	# Wait for executor-c to become unhealthy
	await asyncio.sleep(1.0)

	# Run the reassignment cycle
	await _run_reassignment_cycle(config, monitor_db, dbos_db)

	for i in range(5):
		executor = await _get_workflow_executor(postgres_url, f"wf-multi-{i}")
		assert executor == "executor-d"
