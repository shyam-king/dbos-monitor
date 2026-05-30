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
                        "now": now_ms,
                    },
                )
            ).fetchall()
            return [r[0] for r in rows]

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
