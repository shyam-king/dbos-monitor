from pydantic import BaseModel


class HeartbeatRequest(BaseModel):
	executor_id: str
	executor_type: str
	health_ping_interval_ms: int
	workflow_mappings: dict[str, str] = {}
	active_workflow_count: int = 0


class HeartbeatResponse(BaseModel):
	recovery_needed: bool


class ExecutorInfo(BaseModel):
	executor_id: str
	executor_type: str
	health_ping_interval_ms: int
	last_ping_at: int
	first_seen_at: int
	healthy: bool
	active_workflow_count: int
