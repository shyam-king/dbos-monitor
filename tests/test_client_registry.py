"""Unit tests for the SDK workflow-type registry and its delivery in the heartbeat payload."""

import pytest

from dbos_monitor_client import (
	DBOSMonitorClient,
	register_workflow_mapping,
	register_workflow_type,
	registry,
	set_workflow_mappings,
)


@pytest.fixture(autouse=True)
def _clean_registry():
	registry._REGISTRY.clear()
	yield
	registry._REGISTRY.clear()


def test_register_workflow_mapping_and_snapshot():
	register_workflow_mapping("ingest", "worker")
	snapshot = registry.get_workflow_mappings()
	assert snapshot == {"ingest": "worker"}

	# The snapshot is a copy: mutating it must not affect the registry.
	snapshot["serve"] = "api"
	assert registry.get_workflow_mappings() == {"ingest": "worker"}


def test_set_workflow_mappings_merges_newest_wins():
	register_workflow_mapping("ingest", "worker")
	set_workflow_mappings({"ingest": "batch", "serve": "api"})
	assert registry.get_workflow_mappings() == {"ingest": "batch", "serve": "api"}


def test_decorator_registers_name_and_returns_func():
	@register_workflow_type("worker", "my_workflow")
	def my_workflow():
		return 42

	assert my_workflow() == 42  # decorator is a passthrough
	assert registry.get_workflow_mappings() == {"my_workflow": "worker"}


def test_heartbeat_payload_includes_mappings(monkeypatch):
	register_workflow_mapping("ingest", "worker")
	captured = {}

	class _Resp:
		def raise_for_status(self):
			pass

		def json(self):
			return {"recovery_needed": False}

	def fake_post(url, json, timeout):
		captured["url"] = url
		captured["json"] = json
		return _Resp()

	monkeypatch.setattr("dbos_monitor_client.heartbeat.httpx.post", fake_post)

	client = DBOSMonitorClient(
		monitor_url="http://monitor",
		executor_id="worker-1",
		executor_type="worker",
		health_ping_interval_ms=500,
	)
	client._send_heartbeat()

	assert captured["json"]["workflow_mappings"] == {"ingest": "worker"}
	assert captured["json"]["executor_id"] == "worker-1"
