import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from dbos_monitor.service.config import MonitorConfig
from dbos_monitor.service.dbos_db import DbosDB
from dbos_monitor.service.models import ExecutorInfo, HeartbeatRequest, HeartbeatResponse
from dbos_monitor.service.monitor_db import MonitorDB
from dbos_monitor.service.scheduler import orphan_assignment_loop, reassignment_loop, type_discovery_loop

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
	config = app.state.config
	logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))

	monitor_db = MonitorDB(config.monitor_postgres_connection_uri)
	dbos_db = DbosDB(config.dbos_postgres_connection_uri)
	await monitor_db.connect()
	await dbos_db.connect()

	app.state.monitor_db = monitor_db
	app.state.dbos_db = dbos_db

	tasks = [asyncio.create_task(reassignment_loop(config, monitor_db, dbos_db))]
	if config.enable_experimental_wkflw_type_discovery:
		logger.info("Experimental workflow-type discovery enabled; starting discovery + orphan-assignment loops")
		tasks.append(asyncio.create_task(type_discovery_loop(config, monitor_db, dbos_db)))
		tasks.append(asyncio.create_task(orphan_assignment_loop(config, monitor_db, dbos_db)))
	yield
	for task in tasks:
		task.cancel()
	for task in tasks:
		try:
			await task
		except asyncio.CancelledError:
			pass
	await monitor_db.close()
	await dbos_db.close()


def create_app(config: MonitorConfig | None = None) -> FastAPI:
	if config is None:
		config = MonitorConfig()  # type: ignore[call-arg]
	app = FastAPI(title="dbos-monitor", lifespan=lifespan)
	app.state.config = config
	_register_routes(app)
	return app


def _register_routes(app: FastAPI):

	@app.post("/heartbeat", response_model=HeartbeatResponse)
	async def heartbeat(req: HeartbeatRequest):
		monitor_db: MonitorDB = app.state.monitor_db
		is_new = await monitor_db.upsert_executor(req.executor_id, req.executor_type, req.health_ping_interval_ms)
		if is_new:
			logger.info(
				"Discovered new executor %s (type=%s, ping_interval_ms=%d)",
				req.executor_id,
				req.executor_type,
				req.health_ping_interval_ms,
			)
		recovery_needed = await monitor_db.check_and_clear_recovery_needed(req.executor_id)
		return HeartbeatResponse(recovery_needed=recovery_needed)

	@app.get("/health")
	async def health():
		return {"status": "ok"}

	@app.get("/executors", response_model=list[ExecutorInfo])
	async def list_executors():
		monitor_db: MonitorDB = app.state.monitor_db
		config: MonitorConfig = app.state.config
		executors = await monitor_db.get_all_executors(grace_timeout_ms=config.executor_health_ping_grace_timeout_ms)
		return executors
