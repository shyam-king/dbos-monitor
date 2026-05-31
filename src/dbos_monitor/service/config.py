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
