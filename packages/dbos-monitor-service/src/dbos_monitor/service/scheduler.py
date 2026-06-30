import asyncio
import logging
import random

from dbos_monitor.service.config import MonitorConfig
from dbos_monitor.service.dbos_db import DbosDB
from dbos_monitor.service.monitor_db import MonitorDB

logger = logging.getLogger(__name__)


async def reassignment_loop(config: MonitorConfig, monitor_db: MonitorDB, dbos_db: DbosDB):
	# Executor IDs already warned about, so a still-unhealthy executor (e.g. one with no
	# healthy peer to take over yet) isn't re-logged every cycle.
	warned_unhealthy: set[str] = set()
	while True:
		try:
			await _run_reassignment_cycle(config, monitor_db, dbos_db, warned_unhealthy)
		except asyncio.CancelledError:
			raise
		except Exception:
			logger.exception("Error in reassignment loop")
		await asyncio.sleep(config.reassignment_loop_interval_ms / 1000)


async def _run_reassignment_cycle(
	config: MonitorConfig, monitor_db: MonitorDB, dbos_db: DbosDB, warned_unhealthy: set[str] | None = None
):
	if warned_unhealthy is None:
		warned_unhealthy = set()

	unhealthy = await monitor_db.get_unhealthy_executors(
		grace_timeout_ms=config.executor_health_ping_grace_timeout_ms,
		unknown_timeout_ms=config.unknown_executor_health_ping_timeout_ms,
	)
	# Forget executors that are healthy again so a fresh failure logs anew.
	warned_unhealthy.intersection_update(e.executor_id for e in unhealthy)

	for executor in unhealthy:
		if executor.executor_id not in warned_unhealthy:
			logger.warning(
				"Executor %s (type=%s) is unhealthy (last heartbeat at %d); attempting reassignment",
				executor.executor_id,
				executor.executor_type,
				executor.last_ping_at,
			)
			warned_unhealthy.add(executor.executor_id)
		await _drain_executor(config, monitor_db, dbos_db, executor)


async def _drain_executor(config: MonitorConfig, monitor_db: MonitorDB, dbos_db: DbosDB, executor):
	"""Reassign an unhealthy executor's workflows in batches until it is fully drained.

	Each batch is capped at ``reassignment_max_batch_size`` and sent to a freshly chosen
	random healthy peer, so a large backlog spreads across peers without a per-batch delay.
	A batch smaller than the cap means nothing more is pending, so the executor is removed.
	"""
	batch_size = config.reassignment_max_batch_size
	while True:
		healthy_targets = await monitor_db.get_healthy_executors_by_type(
			executor.executor_type,
			grace_timeout_ms=config.executor_health_ping_grace_timeout_ms,
		)
		if not healthy_targets:
			# No peer to take over yet; leave the executor in place and retry next cycle.
			logger.debug(
				"No healthy executors of type %r to reassign workflows from %s",
				executor.executor_type,
				executor.executor_id,
			)
			return

		target = _select_target(healthy_targets)
		reassigned = await dbos_db.reassign_workflows(
			old_executor_id=executor.executor_id,
			new_executor_id=target.executor_id,
			age_threshold_ms=config.workflows_age_threshold_ms,
			max_batch_size=batch_size,
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

		if len(reassigned) < batch_size:
			await monitor_db.remove_executor(executor.executor_id)
			return


def _select_target(healthy_executors):
	return random.choice(healthy_executors)


async def orphan_assignment_loop(config: MonitorConfig, monitor_db: MonitorDB, dbos_db: DbosDB):
	"""Re-home workflows abandoned by executors the monitor never tracked, using the explicit
	``workflow_name -> executor_type`` mapping (pushed by executors) to find a healthy peer."""
	# Workflows already warned about as unplaceable, so we don't re-log every cycle while they
	# wait for a compatible executor (or for their mapping to be declared).
	warned_unplaceable: set[str] = set()
	while True:
		try:
			await _run_orphan_cycle(config, monitor_db, dbos_db, warned_unplaceable)
		except asyncio.CancelledError:
			raise
		except Exception:
			logger.exception("Error in orphan assignment loop")
		await asyncio.sleep(config.orphan_assignment_loop_interval_ms / 1000)


async def _run_orphan_cycle(
	config: MonitorConfig, monitor_db: MonitorDB, dbos_db: DbosDB, warned_unplaceable: set[str] | None = None
):
	if warned_unplaceable is None:
		warned_unplaceable = set()

	known_ids = await monitor_db.get_all_executor_ids()
	if not known_ids:
		# Guard: with no known executors, every PENDING workflow would look abandoned.
		return

	orphans = await dbos_db.get_pending_workflows_not_owned_by(
		known_ids,
		age_threshold_ms=config.orphan_age_threshold_ms,
		limit=config.orphan_assignment_max_batch_size,
	)
	# Forget workflows that are no longer orphaned so a fresh failure logs anew.
	warned_unplaceable.intersection_update(o["workflow_uuid"] for o in orphans)
	if not orphans:
		logger.debug("Orphan assignment cycle found no abandoned workflows")
		return

	types_by_name = await monitor_db.get_types_for_workflows(list({o["name"] for o in orphans}))
	healthy = await monitor_db.get_all_executors(grace_timeout_ms=config.executor_health_ping_grace_timeout_ms)
	healthy_by_type: dict[str, list[str]] = {}
	for e in healthy:
		if e["healthy"]:
			healthy_by_type.setdefault(e["executor_type"], []).append(e["executor_id"])

	# Group each placeable orphan under a randomly chosen healthy executor of its mapped type.
	assignments: dict[str, list[str]] = {}
	type_for_target: dict[str, str] = {}
	for orphan in orphans:
		executor_type = types_by_name.get(orphan["name"])
		candidates = healthy_by_type.get(executor_type, []) if executor_type else []
		if not candidates:
			if orphan["workflow_uuid"] not in warned_unplaceable:
				logger.warning(
					"Abandoned workflow %s (name=%s) owned by unknown executor %s has no compatible "
					"healthy executor; leaving in place",
					orphan["workflow_uuid"],
					orphan["name"],
					orphan["executor_id"],
				)
				warned_unplaceable.add(orphan["workflow_uuid"])
			continue
		target = random.choice(candidates)
		assignments.setdefault(target, []).append(orphan["workflow_uuid"])
		type_for_target[target] = executor_type

	for target, uuids in assignments.items():
		reassigned = await dbos_db.reassign_workflow_ids(uuids, target)
		if reassigned:
			logger.info(
				"Reassigned %d abandoned workflow(s) to %s (type=%s): %s",
				len(reassigned),
				target,
				type_for_target[target],
				", ".join(reassigned),
			)
			await monitor_db.mark_recovery_needed(target)
