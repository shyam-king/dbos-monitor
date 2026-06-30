from pydantic_settings import BaseSettings


class MonitorConfig(BaseSettings):
	model_config = {"env_prefix": "DBOS_MONITOR_"}

	executor_health_ping_grace_timeout_ms: int = 5000
	unknown_executor_health_ping_timeout_ms: int = 30000
	log_level: str = "INFO"
	workflows_age_threshold_ms: int | None = None
	dbos_postgres_connection_uri: str
	monitor_postgres_connection_uri: str
	reassignment_loop_interval_ms: int = 3000
	reassignment_max_batch_size: int = 20

	# Orphan recovery: re-home workflows abandoned by executors the monitor never tracked,
	# using the explicit workflow->executor_type mapping pushed by executors.
	orphan_assignment_loop_interval_ms: int = 10000
	orphan_assignment_max_batch_size: int = 20
	# Only re-home abandoned workflows older than this (default 2h) so the orphan loop never
	# races freshly-created workflows or an executor the reassignment loop just drained.
	orphan_age_threshold_ms: int | None = 7200000
