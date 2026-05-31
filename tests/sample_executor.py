"""
Sample DBOS executor process for E2E testing.

Usage:
    python sample_executor.py <postgres_url> <executor_id> <monitor_url> [--type <type>] [--start-workflow]

--type sets the executor type reported to the monitor (default "worker"); reassignment
only happens between executors of the same type.
When --start-workflow is passed, starts --num-workflows (default 1) sample workflows and
prints each workflow ID to stdout, one per line.
Otherwise, just launches DBOS (triggering recovery) and waits for workflows to complete.
"""

import asyncio
import logging
import sys

from dbos import DBOS

logger = logging.getLogger("sample_executor")


async def run_executor(
	postgres_url: str,
	executor_id: str,
	monitor_url: str,
	start_workflow: bool,
	executor_type: str = "worker",
	num_workflows: int = 1,
):
	# Logs go to stderr; stdout is reserved for the workflow-ID line the test reads.
	logging.basicConfig(
		level=logging.INFO,
		format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
		stream=sys.stderr,
	)

	logger.info(
		"Initializing DBOS executor %s (type=%s, start_workflow=%s)", executor_id, executor_type, start_workflow
	)
	config = {
		"name": "testapp",
		"system_database_url": postgres_url,
		"executor_id": executor_id,
		"run_admin_server": False,
		"application_version": "v1",
	}

	DBOS(config=config)

	@DBOS.workflow()
	async def multi_step_workflow():
		logger.info("workflow started workflow_id=%s", DBOS.workflow_id)
		r1 = await step_one()
		r2 = await step_two(r1)
		r3 = await step_three(r2)
		logger.info("workflow ended")
		return r3

	@DBOS.step()
	async def step_one():
		logger.info("step_one")
		return "step1_done"

	@DBOS.step()
	async def step_two(prev):
		logger.info("step_two_start")
		await asyncio.sleep(10)
		logger.info("step_two_end")
		return f"{prev}+step2_done"

	@DBOS.step()
	async def step_three(prev):
		logger.info("step_three")
		return f"{prev}+step3_done"

	DBOS.launch()
	logger.info("DBOS launched for executor %s", executor_id)

	from dbos_monitor_client import DBOSMonitorClient

	monitor_client = DBOSMonitorClient(
		monitor_url=monitor_url,
		executor_id=executor_id,
		executor_type=executor_type,
		health_ping_interval_ms=500,
	)
	monitor_client.start()
	logger.info("Heartbeat client started, reporting to %s", monitor_url)

	if start_workflow:
		for _ in range(num_workflows):
			handle = await DBOS.start_workflow_async(multi_step_workflow)
			workflow_id = handle.get_workflow_id()
			logger.info("Started workflow %s", workflow_id)
			print(workflow_id, flush=True)

	logger.info("Executor %s running; waiting for workflows", executor_id)
	try:
		while True:
			await asyncio.sleep(0.1)
	except (KeyboardInterrupt, asyncio.CancelledError):
		pass
	finally:
		logger.info("Shutting down executor %s", executor_id)
		monitor_client.stop()
		DBOS.destroy()


if __name__ == "__main__":
	postgres_url = sys.argv[1]
	executor_id = sys.argv[2]
	monitor_url = sys.argv[3]
	start_wf = "--start-workflow" in sys.argv
	executor_type = sys.argv[sys.argv.index("--type") + 1] if "--type" in sys.argv else "worker"
	num_workflows = int(sys.argv[sys.argv.index("--num-workflows") + 1]) if "--num-workflows" in sys.argv else 1
	asyncio.run(run_executor(postgres_url, executor_id, monitor_url, start_wf, executor_type, num_workflows))
