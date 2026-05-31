"""
Sample DBOS executor process for E2E testing.

Usage:
    python sample_executor.py <postgres_url> <executor_id> <monitor_url> [--start-workflow]

When --start-workflow is passed, starts a sample workflow and prints its ID to stdout.
Otherwise, just launches DBOS (triggering recovery) and waits for workflows to complete.
"""

import sys
import time

from dbos import DBOS


def run_executor(postgres_url: str, executor_id: str, monitor_url: str, start_workflow: bool):
	config = {
		"name": "testapp",
		"system_database_url": postgres_url,
		"executor_id": executor_id,
		"run_admin_server": False,
		"application_version": "v1",
	}

	DBOS(config=config)

	@DBOS.workflow()
	def multi_step_workflow():
		r1 = step_one()
		r2 = step_two(r1)
		r3 = step_three(r2)
		return r3

	@DBOS.step()
	def step_one():
		return "step1_done"

	@DBOS.step()
	def step_two(prev):
		time.sleep(10)
		return f"{prev}+step2_done"

	@DBOS.step()
	def step_three(prev):
		return f"{prev}+step3_done"

	DBOS.launch()

	from dbos_monitor.client.heartbeat import DBOSMonitorClient

	monitor_client = DBOSMonitorClient(
		monitor_url=monitor_url,
		executor_id=executor_id,
		executor_type="worker",
		health_ping_interval_ms=500,
	)
	monitor_client.start()

	if start_workflow:
		handle = DBOS.start_workflow(multi_step_workflow)
		print(handle.get_workflow_id(), flush=True)

	try:
		while True:
			time.sleep(0.1)
	except KeyboardInterrupt:
		pass
	finally:
		monitor_client.stop()
		DBOS.destroy()


if __name__ == "__main__":
	postgres_url = sys.argv[1]
	executor_id = sys.argv[2]
	monitor_url = sys.argv[3]
	start_wf = "--start-workflow" in sys.argv
	run_executor(postgres_url, executor_id, monitor_url, start_wf)
