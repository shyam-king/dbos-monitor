# syntax=docker/dockerfile:1

# --- Build stage: resolve and install dependencies with uv into a venv ---
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
	UV_LINK_MODE=copy \
	UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Install only the service package's dependencies first (cached layer, independent of
# source changes). The workspace manifests are needed to resolve; --package limits the
# install to dbos-monitor-service so the client and its deps are left out of the image.
RUN --mount=type=cache,target=/root/.cache/uv \
	--mount=type=bind,source=uv.lock,target=uv.lock \
	--mount=type=bind,source=pyproject.toml,target=pyproject.toml \
	--mount=type=bind,source=packages/dbos-monitor-service/pyproject.toml,target=packages/dbos-monitor-service/pyproject.toml \
	--mount=type=bind,source=packages/dbos-monitor-client/pyproject.toml,target=packages/dbos-monitor-client/pyproject.toml \
	uv sync --locked --no-install-workspace --no-dev --package dbos-monitor-service

# Install the service package itself.
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
	uv sync --locked --no-dev --package dbos-monitor-service

# --- Runtime stage: slim image carrying only the venv and source ---
FROM python:3.12-slim-bookworm

# Run as an unprivileged user.
RUN useradd --create-home --uid 1000 app

WORKDIR /app
COPY --from=builder --chown=app:app /app /app

# Put the venv on PATH so `uvicorn` resolves to the installed one.
ENV PATH="/app/.venv/bin:$PATH"

USER app
EXPOSE 8000

CMD ["uvicorn", "dbos_monitor.service.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
