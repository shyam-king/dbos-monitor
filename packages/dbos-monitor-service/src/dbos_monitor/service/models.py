from pydantic import BaseModel


class HeartbeatRequest(BaseModel):
	executor_id: str
	executor_type: str
	health_ping_interval_ms: int


class HeartbeatResponse(BaseModel):
	recovery_needed: bool


class ExecutorInfo(BaseModel):
	executor_id: str
	executor_type: str
	health_ping_interval_ms: int
	last_ping_at: int
	first_seen_at: int
	healthy: bool
