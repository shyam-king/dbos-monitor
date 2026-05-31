import time

from psycopg_pool import AsyncConnectionPool


class DbosDB:
	def __init__(self, connection_uri: str):
		self._uri = connection_uri
		self._pool: AsyncConnectionPool | None = None

	async def connect(self):
		self._pool = AsyncConnectionPool(self._uri, min_size=1, max_size=10, open=False)
		await self._pool.open(wait=True)

	async def close(self):
		if self._pool:
			await self._pool.close()

	async def reassign_workflows(
		self,
		old_executor_id: str,
		new_executor_id: str,
		age_threshold_ms: int | None = None,
		max_batch_size: int = 20,
	) -> list[str]:
		now_ms = int(time.time() * 1000)
		async with self._pool.connection() as conn:
			rows = await (
				await conn.execute(
					"""
                    WITH claimed AS (
                        SELECT workflow_uuid
                        FROM dbos.workflow_status
                        WHERE status = 'PENDING'
                          AND executor_id = %(old_id)s
                          AND (%(threshold)s::BIGINT IS NULL OR created_at >= %(threshold)s)
                        ORDER BY created_at
                        LIMIT %(limit)s
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE dbos.workflow_status ws
                    SET executor_id = %(new_id)s,
                        updated_at = %(now)s
                    FROM claimed
                    WHERE ws.workflow_uuid = claimed.workflow_uuid
                    RETURNING ws.workflow_uuid
                    """,
					{
						"old_id": old_executor_id,
						"new_id": new_executor_id,
						"threshold": age_threshold_ms,
						"limit": max_batch_size,
						"now": now_ms,
					},
				)
			).fetchall()
			return [r[0] for r in rows]

	async def reassign_workflow_ids(self, workflow_uuids: list[str], new_executor_id: str) -> list[str]:
		"""Reassign specific still-PENDING workflows to ``new_executor_id``.

		Unlike ``reassign_workflows`` (which drains one old executor wholesale), this targets a
		given set of UUIDs — orphans of different names may go to different executors. Locks the
		same way (FOR UPDATE SKIP LOCKED) and re-checks status='PENDING' so it never overwrites a
		workflow another loop already moved or that started running.
		"""
		if not workflow_uuids:
			return []
		now_ms = int(time.time() * 1000)
		async with self._pool.connection() as conn:
			rows = await (
				await conn.execute(
					"""
                    WITH claimed AS (
                        SELECT workflow_uuid
                        FROM dbos.workflow_status
                        WHERE status = 'PENDING'
                          AND workflow_uuid = ANY(%(ids)s::text[])
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE dbos.workflow_status ws
                    SET executor_id = %(new_id)s,
                        updated_at = %(now)s
                    FROM claimed
                    WHERE ws.workflow_uuid = claimed.workflow_uuid
                    RETURNING ws.workflow_uuid
                    """,
					{"ids": workflow_uuids, "new_id": new_executor_id, "now": now_ms},
				)
			).fetchall()
			return [r[0] for r in rows]

	async def get_completed_workflows_by_executors(
		self, executor_ids: list[str], lookback_ms: int, limit: int
	) -> list[tuple[str, str]]:
		"""Recently SUCCEEDED workflows owned by the given executors, as (name, executor_id).

		Bounded by a rolling lookback window and a row limit so the (unbounded) SUCCESS history
		is never fully scanned — the discovery loop only needs newly-completed workflows.
		"""
		if not executor_ids:
			return []
		since = int(time.time() * 1000) - lookback_ms
		async with self._pool.connection() as conn:
			rows = await (
				await conn.execute(
					"""
                    SELECT name, executor_id
                    FROM dbos.workflow_status
                    WHERE status = 'SUCCESS'
                      AND executor_id = ANY(%(ids)s::text[])
                      AND updated_at >= %(since)s
                      AND name IS NOT NULL
                    LIMIT %(limit)s
                    """,
					{"ids": executor_ids, "since": since, "limit": limit},
				)
			).fetchall()
			return [(r[0], r[1]) for r in rows]

	async def get_pending_workflows_not_owned_by(
		self, known_executor_ids: list[str], age_threshold_ms: int | None, limit: int
	) -> list[dict]:
		"""PENDING workflows whose executor_id is none of ``known_executor_ids`` — i.e. abandoned
		by an executor the monitor never tracked. Only returns workflows older than
		``age_threshold_ms`` (when set) so freshly-created work is left alone.

		WARNING: callers MUST guard against an empty ``known_executor_ids`` — ``<> ALL(ARRAY[])``
		is vacuously true and would match every PENDING workflow in the database.
		"""
		if not known_executor_ids:
			return []
		threshold = None if age_threshold_ms is None else int(time.time() * 1000) - age_threshold_ms
		async with self._pool.connection() as conn:
			rows = await (
				await conn.execute(
					"""
                    SELECT workflow_uuid, name, executor_id
                    FROM dbos.workflow_status
                    WHERE status = 'PENDING'
                      AND executor_id <> ALL(%(ids)s::text[])
                      AND name IS NOT NULL
                      AND (%(threshold)s::BIGINT IS NULL OR created_at <= %(threshold)s)
                    ORDER BY created_at
                    LIMIT %(limit)s
                    """,
					{"ids": known_executor_ids, "threshold": threshold, "limit": limit},
				)
			).fetchall()
			return [{"workflow_uuid": r[0], "name": r[1], "executor_id": r[2]} for r in rows]

	async def get_pending_workflows_for_executor(
		self, executor_id: str, age_threshold_ms: int | None = None
	) -> list[dict]:
		async with self._pool.connection() as conn:
			rows = await (
				await conn.execute(
					"""
                    SELECT workflow_uuid, executor_id, name, created_at
                    FROM dbos.workflow_status
                    WHERE status = 'PENDING'
                      AND executor_id = %(id)s
                      AND (%(threshold)s::BIGINT IS NULL OR created_at >= %(threshold)s)
                    """,
					{"id": executor_id, "threshold": age_threshold_ms},
				)
			).fetchall()
			return [
				{
					"workflow_uuid": r[0],
					"executor_id": r[1],
					"name": r[2],
					"created_at": r[3],
				}
				for r in rows
			]
