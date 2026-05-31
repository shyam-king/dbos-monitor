"""
Full end-to-end test with real DBOS executors.

Spins up:
- Postgres (via testcontainers or TEST_POSTGRES_URL)
- dbos-monitor service (real HTTP server via uvicorn)
- Executor A: starts a long-running workflow, sends real heartbeats, gets killed
- Executor B: comes up, sends real heartbeats, gets reassignment, recovers the workflow

Requires: Docker (for testcontainers) or TEST_POSTGRES_URL env var.
"""

import asyncio
import os
import signal
import subprocess
import sys
import threading
import time

import psycopg
import pytest
import uvicorn

from dbos_monitor.service.app import create_app
from dbos_monitor.service.config import MonitorConfig

pytestmark = pytest.mark.integration

SAMPLE_EXECUTOR = os.path.join(os.path.dirname(__file__), "sample_executor.py")
MONITOR_PORT = 18321


@pytest.fixture
def monitor_config(postgres_url):
	return MonitorConfig(
		executor_health_ping_grace_timeout_ms=1000,
		unknown_executor_health_ping_timeout_ms=3000,
		dbos_postgres_connection_uri=postgres_url,
		monitor_postgres_connection_uri=postgres_url,
		reassignment_loop_interval_ms=500,
	)


@pytest.fixture
async def monitor_server(monitor_config, postgres_url):
	# Clean slate: drop everything so DBOS can run its own migrations
	async with await psycopg.AsyncConnection.connect(postgres_url, autocommit=True) as conn:
		await conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
		await conn.execute("DROP TABLE IF EXISTS executors")

	app = create_app(monitor_config)
	config = uvicorn.Config(app, host="127.0.0.1", port=MONITOR_PORT, log_level="warning")
	server = uvicorn.Server(config)
	thread = threading.Thread(target=server.run, daemon=True)
	thread.start()

	for _ in range(50):
		if server.started:
			break
		await asyncio.sleep(0.1)

	yield f"http://127.0.0.1:{MONITOR_PORT}"

	server.should_exit = True
	thread.join(timeout=5)


def _start_executor(postgres_url: str, executor_id: str, monitor_url: str, start_workflow: bool):
	args = [sys.executable, SAMPLE_EXECUTOR, postgres_url, executor_id, monitor_url]
	if start_workflow:
		args.append("--start-workflow")
	proc = subprocess.Popen(
		args,
		stdout=subprocess.PIPE,
		stderr=subprocess.PIPE,
		text=True,
	)
	return proc


async def _get_workflow_status(postgres_url: str, workflow_id: str) -> str | None:
	async with await psycopg.AsyncConnection.connect(postgres_url, autocommit=True) as conn:
		row = await (
			await conn.execute(
				"SELECT status FROM dbos.workflow_status WHERE workflow_uuid = %(id)s",
				{"id": workflow_id},
			)
		).fetchone()
		return row[0] if row else None


async def _get_workflow_executor(postgres_url: str, workflow_id: str) -> str | None:
	async with await psycopg.AsyncConnection.connect(postgres_url, autocommit=True) as conn:
		row = await (
			await conn.execute(
				"SELECT executor_id FROM dbos.workflow_status WHERE workflow_uuid = %(id)s",
				{"id": workflow_id},
			)
		).fetchone()
		return row[0] if row else None


async def test_full_executor_crash_and_recovery(monitor_server, postgres_url):
	"""
	Full E2E: executor-a starts a workflow, crashes, monitor reassigns,
	executor-b picks it up via real heartbeats and DBOS recovery.
	"""
	monitor_url = monitor_server

	# Start executor-a: sends real heartbeats to the monitor, starts a long-running workflow
	proc_a = _start_executor(postgres_url, "executor-a", monitor_url, True)

	# Read the workflow ID from stdout (blocks until printed)
	workflow_id = proc_a.stdout.readline().strip()
	assert workflow_id, f"Failed to get workflow ID. stderr: {proc_a.stderr.read()}"

	# Give it a moment to start executing and send a few heartbeats
	await asyncio.sleep(2)

	# Verify workflow is PENDING and executor-a is registered
	status = await _get_workflow_status(postgres_url, workflow_id)
	assert status == "PENDING"

	# Kill executor-a (simulating crash — heartbeats stop)
	os.kill(proc_a.pid, signal.SIGKILL)
	proc_a.wait()

	# Wait for executor-a to be considered unhealthy by the monitor
	# (ping_interval 500ms + grace 1000ms = 1.5s, plus reassignment loop runs every 500ms)
	await asyncio.sleep(3.0)

	# Start executor-b: sends real heartbeats to the monitor
	# DBOS on launch() auto-recovers PENDING workflows for this executor_id
	proc_b = _start_executor(postgres_url, "executor-b", monitor_url, False)

	# Wait for the monitor to reassign and executor-b to recover the workflow
	# Reassignment happens when: executor-a is unhealthy + executor-b is registered (healthy)
	# Then executor-b's next heartbeat gets recovery_needed=true, triggering DBOS recovery
	deadline = time.time() + 25
	final_status = None
	while time.time() < deadline:
		final_status = await _get_workflow_status(postgres_url, workflow_id)
		if final_status == "SUCCESS":
			break
		await asyncio.sleep(1.0)

	# Clean up executor-b
	os.kill(proc_b.pid, signal.SIGKILL)
	_, proc_b_stderr = proc_b.communicate(timeout=5)

	# Verify the workflow completed successfully
	assert final_status == "SUCCESS", (
		f"Workflow did not complete. Status: {final_status}\nexecutor-b stderr:\n{proc_b_stderr[-2000:]}"
	)

	# Verify the workflow ended up assigned to executor-b
	final_executor = await _get_workflow_executor(postgres_url, workflow_id)
	assert final_executor == "executor-b"
