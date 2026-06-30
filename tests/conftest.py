import contextlib
import os

import psycopg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

DOCKER_SETUP_INSTRUCTIONS = """\
This test needs a Postgres database. Provide one of the following:

  1. Set TEST_POSTGRES_URL to point at a running Postgres instance, e.g.:
       export TEST_POSTGRES_URL="postgresql://postgres:postgres@localhost:5432/postgres"

  2. Or run Docker so testcontainers can spin up Postgres automatically.
     If Docker is running but not auto-detected, set DOCKER_HOST, e.g.:
       export DOCKER_HOST="unix://$HOME/.docker/run/docker.sock"   # Docker Desktop on macOS
       export DOCKER_HOST="unix:///var/run/docker.sock"            # Linux
"""


@pytest.fixture(scope="session")
def postgres_url():
	url = os.environ.get("TEST_POSTGRES_URL")
	if url:
		yield url
		return

	container = None
	try:
		container = PostgresContainer("postgres:16-alpine", driver=None)
		container.start()
	except Exception as exc:
		if container is not None:
			# clean up if start() failed after the container was created
			with contextlib.suppress(Exception):
				container.stop()
		pytest.skip(f"Could not start Postgres via Docker: {exc}\n\n{DOCKER_SETUP_INSTRUCTIONS}")

	try:
		host = container.get_container_host_ip()
		port = container.get_exposed_port(5432)
		yield f"postgresql://{container.username}:{container.password}@{host}:{port}/{container.dbname}"
	finally:
		container.stop()


@pytest_asyncio.fixture(scope="session")
async def _init_schema(postgres_url):
	async with await psycopg.AsyncConnection.connect(postgres_url, autocommit=True) as conn:
		await conn.execute("CREATE SCHEMA IF NOT EXISTS dbos")
		await conn.execute("""
            CREATE TABLE IF NOT EXISTS dbos.workflow_status (
                workflow_uuid TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                name TEXT,
                class_name VARCHAR(255),
                config_name VARCHAR(255),
                executor_id TEXT,
                recovery_attempts BIGINT DEFAULT 0,
                application_version TEXT,
                application_id TEXT,
                output TEXT,
                error TEXT,
                queue_name TEXT,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL,
                inputs TEXT
            )
        """)
		await conn.execute("""
            CREATE INDEX IF NOT EXISTS workflow_status_executor_id_index
            ON dbos.workflow_status (executor_id)
        """)
		await conn.execute("""
            CREATE INDEX IF NOT EXISTS workflow_status_status_index
            ON dbos.workflow_status (status)
        """)


@pytest_asyncio.fixture()
async def clean_tables(postgres_url, _init_schema):
	yield
	async with await psycopg.AsyncConnection.connect(postgres_url, autocommit=True) as conn:
		await conn.execute("DELETE FROM dbos.workflow_status")
		await conn.execute("DROP TABLE IF EXISTS executors")
		await conn.execute("DROP TABLE IF EXISTS workflow_type_mapping")
