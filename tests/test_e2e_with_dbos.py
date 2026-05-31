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
import logging
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
REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
MONITOR_PORT = 18321

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


@pytest.fixture
def e2e_log_dir(request):
	"""A gitignored directory under the repo root collecting all logs for this test run."""
	log_dir = os.path.join(REPO_ROOT, "e2e-logs", request.node.name)
	os.makedirs(log_dir, exist_ok=True)
	return log_dir


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
async def monitor_server(monitor_config, postgres_url, e2e_log_dir):
	# Clean slate: drop everything so DBOS can run its own migrations
	async with await psycopg.AsyncConnection.connect(postgres_url, autocommit=True) as conn:
		await conn.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
		await conn.execute("DROP TABLE IF EXISTS executors")

	# Capture the in-process monitor/dbos logs into the run's log directory.
	root_logger = logging.getLogger()
	handler = logging.FileHandler(os.path.join(e2e_log_dir, "monitor.log"))
	handler.setFormatter(logging.Formatter(LOG_FORMAT))
	prev_level = root_logger.level
	root_logger.addHandler(handler)
	root_logger.setLevel(logging.INFO)

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

	root_logger.removeHandler(handler)
	root_logger.setLevel(prev_level)
	handler.close()


def _start_executor(
	postgres_url: str,
	executor_id: str,
	monitor_url: str,
	start_workflow: bool,
	log_dir: str,
	executor_type: str = "worker",
):
	args = [sys.executable, SAMPLE_EXECUTOR, postgres_url, executor_id, monitor_url, "--type", executor_type]
	if start_workflow:
		args.append("--start-workflow")
	# Executor logs go to stderr -> a per-executor file; stdout (PIPE) carries the workflow ID.
	# A restarted executor reuses its ID, so append rather than truncate its log.
	log_file = open(os.path.join(log_dir, f"{executor_id}.log"), "a")
	proc = subprocess.Popen(
		args,
		stdout=subprocess.PIPE,
		stderr=log_file,
		text=True,
	)
	return proc, log_file


def _kill(proc, log_file):
	os.kill(proc.pid, signal.SIGKILL)
	proc.wait(timeout=5)
	if proc.stdout:
		proc.stdout.close()
	log_file.close()


async def _await_all_success(postgres_url: str, workflow_ids: list[str], timeout: float) -> dict[str, str | None]:
	deadline = time.time() + timeout
	statuses: dict[str, str | None] = {}
	while time.time() < deadline:
		statuses = {wf: await _get_workflow_status(postgres_url, wf) for wf in workflow_ids}
		if all(s == "SUCCESS" for s in statuses.values()):
			break
		await asyncio.sleep(1.0)
	return statuses


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


async def test_full_executor_crash_and_recovery(monitor_server, postgres_url, e2e_log_dir):
	"""
	Full E2E: executor-a starts a workflow, crashes, monitor reassigns,
	executor-b picks it up via real heartbeats and DBOS recovery.
	"""
	monitor_url = monitor_server

	# Start executor-a: sends real heartbeats to the monitor, starts a long-running workflow
	proc_a, log_a = _start_executor(postgres_url, "executor-a", monitor_url, True, e2e_log_dir)

	# Read the workflow ID from stdout (blocks until printed, or the process exits)
	workflow_id = proc_a.stdout.readline().strip()
	assert workflow_id, f"Failed to get workflow ID. See {log_a.name}"

	# Give it a moment to start executing and send a few heartbeats
	await asyncio.sleep(2)

	# Verify workflow is PENDING and executor-a is registered
	status = await _get_workflow_status(postgres_url, workflow_id)
	assert status == "PENDING"

	# Kill executor-a (simulating crash — heartbeats stop)
	os.kill(proc_a.pid, signal.SIGKILL)
	proc_a.wait()
	proc_a.stdout.close()
	log_a.close()

	# Wait for executor-a to be considered unhealthy by the monitor
	# (ping_interval 500ms + grace 1000ms = 1.5s, plus reassignment loop runs every 500ms)
	await asyncio.sleep(3.0)

	# Start executor-b: sends real heartbeats to the monitor
	# DBOS on launch() auto-recovers PENDING workflows for this executor_id
	proc_b, log_b = _start_executor(postgres_url, "executor-b", monitor_url, False, e2e_log_dir)

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
	proc_b.wait(timeout=5)
	proc_b.stdout.close()
	log_b.close()

	# Verify the workflow completed successfully
	assert final_status == "SUCCESS", f"Workflow did not complete. Status: {final_status}\nSee logs in {e2e_log_dir}"

	# Verify the workflow ended up assigned to executor-b
	final_executor = await _get_workflow_executor(postgres_url, workflow_id)
	assert final_executor == "executor-b"


async def test_multi_type_concurrent_crash_and_recovery(monitor_server, postgres_url, e2e_log_dir):
	"""
	Three executor types run concurrently, each exercising a distinct recovery path.
	Because reassignment is scoped per type, the scenarios stay isolated from one another:

	  type1 (2 execs): type1-a runs a workflow then crashes; its same-type peer type1-b
	                   (a hot standby) is reassigned the workflow and recovers it.
	  type2 (2 execs): type2-a runs a workflow; BOTH type2 execs are killed (so the workflow
	                   has no live same-type owner and is NOT reassigned), then both restart
	                   and the workflow is recovered once the type comes back.
	  type3 (3 execs): type3-a runs a workflow then crashes; one of the two peers picks it up.
	"""
	monitor_url = monitor_server

	# Launch all seven executors concurrently across the three types.
	t1a, log_t1a = _start_executor(postgres_url, "type1-a", monitor_url, True, e2e_log_dir, "type1")
	t1b, log_t1b = _start_executor(postgres_url, "type1-b", monitor_url, False, e2e_log_dir, "type1")
	t2a, log_t2a = _start_executor(postgres_url, "type2-a", monitor_url, True, e2e_log_dir, "type2")
	t2b, log_t2b = _start_executor(postgres_url, "type2-b", monitor_url, False, e2e_log_dir, "type2")
	t3a, log_t3a = _start_executor(postgres_url, "type3-a", monitor_url, True, e2e_log_dir, "type3")
	t3b, log_t3b = _start_executor(postgres_url, "type3-b", monitor_url, False, e2e_log_dir, "type3")
	t3c, log_t3c = _start_executor(postgres_url, "type3-c", monitor_url, False, e2e_log_dir, "type3")

	# Read the workflow IDs from the three workflow-owning executors (blocks until printed).
	wf1 = t1a.stdout.readline().strip()
	wf2 = t2a.stdout.readline().strip()
	wf3 = t3a.stdout.readline().strip()
	assert wf1 and wf2 and wf3, f"Missing workflow IDs. See logs in {e2e_log_dir}"

	# Let the workflows start executing and every executor register a few heartbeats.
	await asyncio.sleep(2)
	for wf in (wf1, wf2, wf3):
		assert await _get_workflow_status(postgres_url, wf) == "PENDING"

	# Scenarios 1 & 3: crash the workflow-owning executor; a healthy same-type peer takes over.
	_kill(t1a, log_t1a)
	_kill(t3a, log_t3a)

	# Scenario 2: kill BOTH type2 executors. With no healthy type2 target, the monitor cannot
	# reassign type2-a's workflow, so it stays owned by type2-a throughout the outage.
	_kill(t2a, log_t2a)
	_kill(t2b, log_t2b)

	# Wait out the health grace period, then bring the whole type2 fleet back. type2-a recovers
	# its own workflow on launch; a restart-ordering race may instead let type2-b reassign-recover
	# it — either way it must be a type2 executor.
	await asyncio.sleep(3.0)
	t2a, log_t2a = _start_executor(postgres_url, "type2-a", monitor_url, False, e2e_log_dir, "type2")
	t2b, log_t2b = _start_executor(postgres_url, "type2-b", monitor_url, False, e2e_log_dir, "type2")

	# Wait for all three workflows to complete.
	statuses = await _await_all_success(postgres_url, [wf1, wf2, wf3], timeout=60)

	# Clean up the remaining live executors.
	for proc, log in [(t1b, log_t1b), (t2a, log_t2a), (t2b, log_t2b), (t3b, log_t3b), (t3c, log_t3c)]:
		_kill(proc, log)

	assert statuses == {wf1: "SUCCESS", wf2: "SUCCESS", wf3: "SUCCESS"}, (
		f"Not all workflows completed: {statuses}\nSee logs in {e2e_log_dir}"
	)

	# Verify each workflow was recovered by an appropriate same-type executor.
	assert await _get_workflow_executor(postgres_url, wf1) == "type1-b"
	assert await _get_workflow_executor(postgres_url, wf2) in {"type2-a", "type2-b"}
	assert await _get_workflow_executor(postgres_url, wf3) in {"type3-b", "type3-c"}
