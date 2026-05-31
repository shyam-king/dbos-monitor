import asyncio
import logging

from dbos_monitor.service.config import MonitorConfig
from dbos_monitor.service.dbos_db import DbosDB
from dbos_monitor.service.monitor_db import MonitorDB

logger = logging.getLogger(__name__)


async def reassignment_loop(config: MonitorConfig, monitor_db: MonitorDB, dbos_db: DbosDB):
	while True:
		try:
			await _run_reassignment_cycle(config, monitor_db, dbos_db)
		except asyncio.CancelledError:
			raise
		except Exception:
			logger.exception("Error in reassignment loop")
		await asyncio.sleep(config.reassignment_loop_interval_ms / 1000)


async def _run_reassignment_cycle(config: MonitorConfig, monitor_db: MonitorDB, dbos_db: DbosDB):
	unhealthy = await monitor_db.get_unhealthy_executors(
		grace_timeout_ms=config.executor_health_ping_grace_timeout_ms,
		unknown_timeout_ms=config.unknown_executor_health_ping_timeout_ms,
	)

	for executor in unhealthy:
		healthy_targets = await monitor_db.get_healthy_executors_by_type(
			executor.executor_type,
			grace_timeout_ms=config.executor_health_ping_grace_timeout_ms,
		)
		if not healthy_targets:
			logger.debug(
				"No healthy executors of type %r to reassign workflows from %s",
				executor.executor_type,
				executor.executor_id,
			)
			continue

		target = _select_target(healthy_targets)
		reassigned = await dbos_db.reassign_workflows(
			old_executor_id=executor.executor_id,
			new_executor_id=target.executor_id,
			age_threshold_ms=config.workflows_age_threshold_ms,
		)

		if reassigned:
			logger.info(
				"Reassigned %d workflows from %s to %s: %s",
				len(reassigned),
				executor.executor_id,
				target.executor_id,
				", ".join(reassigned),
			)
			await monitor_db.mark_recovery_needed(target.executor_id)
			await monitor_db.remove_executor(executor.executor_id)
		else:
			await monitor_db.remove_executor(executor.executor_id)


def _select_target(healthy_executors):
	return healthy_executors[0]
