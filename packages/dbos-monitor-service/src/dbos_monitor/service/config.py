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

	# Experimental: infer which executor types can run which workflows from completed
	# workflows, and use that to re-home workflows abandoned by executors the monitor never
	# tracked. Off by default; gated by DBOS_MONITOR_ENABLE_EXPERIMENTAL_WKFLW_TYPE_DISCOVERY.
	enable_experimental_wkflw_type_discovery: bool = False
	type_discovery_loop_interval_ms: int = 10000
	type_discovery_lookback_ms: int = 60000
	type_discovery_max_batch_size: int = 100
	orphan_assignment_loop_interval_ms: int = 10000
	orphan_assignment_max_batch_size: int = 20
	# Only re-home abandoned workflows older than this (default 2h) so the orphan loop never
	# races freshly-created workflows or an executor the reassignment loop just drained.
	orphan_age_threshold_ms: int | None = 7200000
