import time
from dataclasses import dataclass

from psycopg_pool import AsyncConnectionPool

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS executors (
    executor_id TEXT PRIMARY KEY,
    executor_type TEXT NOT NULL,
    health_ping_interval_ms INT NOT NULL,
    last_ping_at BIGINT NOT NULL,
    first_seen_at BIGINT NOT NULL,
    recovery_needed BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS workflow_type_inference (
    workflow_name TEXT NOT NULL,
    executor_type TEXT NOT NULL,
    last_seen_at BIGINT NOT NULL,
    PRIMARY KEY (workflow_name, executor_type)
);
"""


@dataclass
class ExecutorRecord:
	executor_id: str
	executor_type: str
	health_ping_interval_ms: int
	last_ping_at: int
	first_seen_at: int
	recovery_needed: bool


class MonitorDB:
	def __init__(self, connection_uri: str):
		self._uri = connection_uri
		self._pool: AsyncConnectionPool | None = None

	async def connect(self):
		self._pool = AsyncConnectionPool(self._uri, min_size=1, max_size=10, open=False)
		await self._pool.open(wait=True)
		async with self._pool.connection() as conn:
			await conn.execute(SCHEMA_SQL)

	async def close(self):
		if self._pool:
			await self._pool.close()

	async def upsert_executor(self, executor_id: str, executor_type: str, ping_interval_ms: int) -> bool:
		"""Record a heartbeat. Returns True if this executor was newly discovered (inserted)."""
		now_ms = _now_ms()
		async with self._pool.connection() as conn:
			row = await (
				await conn.execute(
					"""
                INSERT INTO executors (executor_id, executor_type, health_ping_interval_ms, last_ping_at, first_seen_at)
                VALUES (%(id)s, %(type)s, %(interval)s, %(now)s, %(now)s)
                ON CONFLICT (executor_id) DO UPDATE SET
                    last_ping_at = %(now)s,
                    health_ping_interval_ms = %(interval)s
                RETURNING (xmax = 0) AS inserted
                """,
					{"id": executor_id, "type": executor_type, "interval": ping_interval_ms, "now": now_ms},
				)
			).fetchone()
			return bool(row[0])

	async def check_and_clear_recovery_needed(self, executor_id: str) -> bool:
		async with self._pool.connection() as conn:
			row = await (
				await conn.execute(
					"""
                    UPDATE executors
                    SET recovery_needed = FALSE
                    WHERE executor_id = %(id)s AND recovery_needed = TRUE
                    RETURNING executor_id
                    """,
					{"id": executor_id},
				)
			).fetchone()
			return row is not None

	async def get_unhealthy_executors(self, grace_timeout_ms: int, unknown_timeout_ms: int) -> list[ExecutorRecord]:
		now_ms = _now_ms()
		async with self._pool.connection() as conn:
			rows = await (
				await conn.execute(
					"""
                    SELECT executor_id, executor_type, health_ping_interval_ms,
                           last_ping_at, first_seen_at, recovery_needed
                    FROM executors
                    WHERE (last_ping_at + health_ping_interval_ms + %(grace)s) < %(now)s
                       OR (first_seen_at = last_ping_at AND (first_seen_at + %(unknown)s) < %(now)s)
                    """,
					{"grace": grace_timeout_ms, "unknown": unknown_timeout_ms, "now": now_ms},
				)
			).fetchall()
			return [
				ExecutorRecord(
					executor_id=r[0],
					executor_type=r[1],
					health_ping_interval_ms=r[2],
					last_ping_at=r[3],
					first_seen_at=r[4],
					recovery_needed=r[5],
				)
				for r in rows
			]

	async def get_healthy_executors_by_type(self, executor_type: str, grace_timeout_ms: int) -> list[ExecutorRecord]:
		now_ms = _now_ms()
		async with self._pool.connection() as conn:
			rows = await (
				await conn.execute(
					"""
                    SELECT executor_id, executor_type, health_ping_interval_ms,
                           last_ping_at, first_seen_at, recovery_needed
                    FROM executors
                    WHERE executor_type = %(type)s
                      AND (last_ping_at + health_ping_interval_ms + %(grace)s) >= %(now)s
                    """,
					{"type": executor_type, "grace": grace_timeout_ms, "now": now_ms},
				)
			).fetchall()
			return [
				ExecutorRecord(
					executor_id=r[0],
					executor_type=r[1],
					health_ping_interval_ms=r[2],
					last_ping_at=r[3],
					first_seen_at=r[4],
					recovery_needed=r[5],
				)
				for r in rows
			]

	async def mark_recovery_needed(self, executor_id: str) -> None:
		async with self._pool.connection() as conn:
			await conn.execute(
				"UPDATE executors SET recovery_needed = TRUE WHERE executor_id = %(id)s",
				{"id": executor_id},
			)

	async def remove_executor(self, executor_id: str) -> None:
		async with self._pool.connection() as conn:
			await conn.execute(
				"DELETE FROM executors WHERE executor_id = %(id)s",
				{"id": executor_id},
			)

	async def get_all_executor_ids(self) -> list[str]:
		"""Every known executor_id (no health filter). Used to tell genuine orphans
		(owned by an executor the monitor has never tracked) from tracked-but-unhealthy ones."""
		async with self._pool.connection() as conn:
			rows = await (await conn.execute("SELECT executor_id FROM executors")).fetchall()
			return [r[0] for r in rows]

	async def record_workflow_type(self, workflow_name: str, executor_type: str) -> bool:
		"""Record that ``executor_type`` can run ``workflow_name``. Returns True if this is a
		newly discovered mapping (so callers can log only first-time discoveries)."""
		now_ms = _now_ms()
		async with self._pool.connection() as conn:
			row = await (
				await conn.execute(
					"""
                INSERT INTO workflow_type_inference (workflow_name, executor_type, last_seen_at)
                VALUES (%(name)s, %(type)s, %(now)s)
                ON CONFLICT (workflow_name, executor_type) DO UPDATE SET last_seen_at = %(now)s
                RETURNING (xmax = 0) AS inserted
                """,
					{"name": workflow_name, "type": executor_type, "now": now_ms},
				)
			).fetchone()
			return bool(row[0])

	async def get_types_for_workflows(self, workflow_names: list[str]) -> dict[str, list[str]]:
		"""Map each given workflow name to the executor types known to run it."""
		if not workflow_names:
			return {}
		async with self._pool.connection() as conn:
			rows = await (
				await conn.execute(
					"""
                    SELECT workflow_name, executor_type
                    FROM workflow_type_inference
                    WHERE workflow_name = ANY(%(names)s::text[])
                    """,
					{"names": workflow_names},
				)
			).fetchall()
			result: dict[str, list[str]] = {}
			for name, executor_type in rows:
				result.setdefault(name, []).append(executor_type)
			return result

	async def get_all_executors(self, grace_timeout_ms: int) -> list[dict]:
		now_ms = _now_ms()
		async with self._pool.connection() as conn:
			rows = await (
				await conn.execute(
					"""
                    SELECT executor_id, executor_type, health_ping_interval_ms,
                           last_ping_at, first_seen_at,
                           (last_ping_at + health_ping_interval_ms + %(grace)s) >= %(now)s AS healthy
                    FROM executors
                    """,
					{"grace": grace_timeout_ms, "now": now_ms},
				)
			).fetchall()
			return [
				{
					"executor_id": r[0],
					"executor_type": r[1],
					"health_ping_interval_ms": r[2],
					"last_ping_at": r[3],
					"first_seen_at": r[4],
					"healthy": r[5],
				}
				for r in rows
			]


def _now_ms() -> int:
	return int(time.time() * 1000)
