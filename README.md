# WARNING: Project in initial stage

This project has just started and is in its early stages hence there is lack of documentation, contribution guidelines etc. Please get in touch with the author directly for help.

# About the project

This project provides support to applications that use [DBOS-transact python library](https://github.com/dbos-inc/dbos-transact-py).

This project aims to solve gaps in the library (which is what DBOS Conductor does but it is a proprietary software).

## Problems this project aims to tackle

> This section describes the current scope of work this project is taking up but might change as it matures.

### Re-assignment of Workflows to Executors

DBOS-Transact supports recovery of workflows. This recovery mechanism works in this way:
- When an executor runs a workflow, it initially claims it
	- DBOS keeps track of the executor_id against the workflow ID
- Now, if the executor fails and restarts, DBOS runtime picks up all workflows that were assigned to the executor and runs it
- This only works when executor_id is stable
- Stable executor_id assignment and management is difficult when it comes to environments where workers auto-scale, etc.
	- DBOS does not work well with duplicate executor_id. Processed with same executor_id essentially race to claim the recovery of workflow resulting in duplicate execution but single state in the dbos data. (Workflow runs twice but only one output is persisted).

#### Approach

This project introduces 2 components to solve the issue. An HTTP service and a very thin python sdk that interacts with the service.

- Executors use the sdk to register themselves with the monitor service.
- The SDK sets up an independent thread that constantly pings the monitor service denoting the readiness of the executor. (this ping payload contains executor_id).
- The monitor service keeps track of all healthy executors using the pings.
- The monitor service also connects to the same postgres db that is used by the dbos runtime.

## Architecture

```
Executor (DBOS app)                    dbos-monitor service
    │                                        │
    │──── POST /heartbeat ──────────────────►│  (every ping_interval ms)
    │     {executor_id, executor_type,       │
    │      health_ping_interval_ms}          │
    │◄─── 200 {recovery_needed: bool} ──────│
    │                                        │
    │  if recovery_needed:                   │
    │  trigger DBOS recovery                 │
    │                                        │
    │                                        │──── [background loop] ────┐
    │                                        │  1. Check executor health │
    │                                        │  2. Find orphaned workflows│
    │                                        │  3. Reassign to healthy   │
    │                                        │◄──────────────────────────┘
```

## How It Works

1. **Health Tracking** — Executors send periodic heartbeats to the monitor. The monitor stores the last ping time per executor in its own Postgres database.
2. **Unhealthy Detection** — A background loop checks which executors have missed their heartbeat deadline (`ping_interval + grace_timeout`).
3. **Concurrent-Safe Reassignment** — Workflows owned by unhealthy executors are atomically reassigned to a healthy executor of the same type using `FOR UPDATE SKIP LOCKED` (safe for multiple monitor replicas).
4. **Recovery Trigger** — The target executor's next heartbeat response includes `recovery_needed: true`. The client SDK uses this signal to trigger DBOS's built-in recovery mechanism.

## Quick Start

### Running the Monitor Service

```bash
# Install
uv sync

# Run
DBOS_MONITOR_DBOS_POSTGRES_CONNECTION_URI="postgresql://user:pass@host:5432/dbos_db" \
DBOS_MONITOR_MONITOR_POSTGRES_CONNECTION_URI="postgresql://user:pass@host:5432/monitor_db" \
uv run uvicorn dbos_monitor.service.app:create_app --factory --host 0.0.0.0 --port 8000
```

### Integrating the Client SDK

```python
from dbos_monitor.client.heartbeat import DBOSMonitorClient

# The default on_recovery_needed handler automatically triggers DBOS recovery,
# so no custom callback is needed for standard usage.
client = DBOSMonitorClient(
    monitor_url="http://localhost:8000",
    executor_id="my-executor-id",
    executor_type="worker",
    health_ping_interval_ms=5000,
)
client.start()
```

If you need custom recovery logic, pass your own callback:

```python
from dbos_monitor.client.heartbeat import DBOSMonitorClient

def my_recovery_callback():
    # Custom recovery logic here
    ...

client = DBOSMonitorClient(
    monitor_url="http://localhost:8000",
    executor_id="my-executor-id",
    executor_type="worker",
    health_ping_interval_ms=5000,
    on_recovery_needed=my_recovery_callback,
)
client.start()
```

## Configuration

### Monitor Service (env vars, prefixed with `DBOS_MONITOR_`)

| Variable | Description | Default |
|----------|-------------|---------|
| `DBOS_MONITOR_DBOS_POSTGRES_CONNECTION_URI` | Connection URI to the DBOS system database | *required* |
| `DBOS_MONITOR_MONITOR_POSTGRES_CONNECTION_URI` | Connection URI for the monitor's own state | *required* |
| `DBOS_MONITOR_EXECUTOR_HEALTH_PING_GRACE_TIMEOUT_MS` | Grace period before declaring unhealthy | `5000` |
| `DBOS_MONITOR_UNKNOWN_EXECUTOR_HEALTH_PING_TIMEOUT_MS` | Timeout for first ping from discovered executor | `30000` |
| `DBOS_MONITOR_WORKFLOWS_AGE_THRESHOLD_MS` | Ignore workflows older than this (epoch ms) | `None` |
| `DBOS_MONITOR_REASSIGNMENT_LOOP_INTERVAL_MS` | How often the reassignment loop runs | `3000` |
| `DBOS_MONITOR_LOG_LEVEL` | Logging level | `INFO` |

### Client SDK Parameters

| Parameter | Description |
|-----------|-------------|
| `monitor_url` | URL of the dbos-monitor service |
| `executor_id` | This executor's unique identifier |
| `executor_type` | Type grouping (workflows only reassigned within same type) |
| `health_ping_interval_ms` | Heartbeat interval in milliseconds |
| `on_recovery_needed` | Callback invoked when recovery is triggered |

## Development

```bash
# Install with dev dependencies
uv sync --all-extras
```

### Running Tests

Unit tests (no Docker required):

```bash
uv run pytest tests/test_logic.py -v
```

Integration + E2E tests require a Postgres instance. Either provide one manually:

```bash
# Start postgres
docker run -d --name dbos-test-pg \
  -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test \
  -p 5433:5432 postgres:16-alpine

# Run all tests against it
TEST_POSTGRES_URL="postgresql://test:test@localhost:5433/test" uv run pytest tests/ -v
```

Or let testcontainers manage it automatically (requires Docker socket access):

```bash
DOCKER_HOST=unix://$HOME/.docker/run/docker.sock uv run pytest tests/ -v
```

> **Why `DOCKER_HOST`?** The Docker CLI may work via its configured context, but the
> Python SDK that testcontainers uses defaults to `/var/run/docker.sock`. On Docker
> Desktop for macOS that socket doesn't exist, so you must point `DOCKER_HOST` at the
> real socket:
>
> - macOS (Docker Desktop): `unix://$HOME/.docker/run/docker.sock`
> - Linux: `unix:///var/run/docker.sock`
>
> If neither `TEST_POSTGRES_URL` nor a reachable Docker socket is available, the
> integration and E2E tests are **skipped** (not failed) with these same instructions.

To avoid prefixing every command, use [direnv](https://direnv.net/) to set `DOCKER_HOST`
automatically whenever you enter the project directory. Create a `.envrc` file in the
project root:

```bash
# .envrc — adjust the socket path for your platform (see above)
export DOCKER_HOST=unix://$HOME/.docker/run/docker.sock
```

Then allow it once:

```bash
direnv allow
```

Now `uv run pytest tests/ -v` picks up `DOCKER_HOST` without any prefix. `.envrc` is
git-ignored, so your local socket path stays out of version control.

# Authors
- shyam-king
