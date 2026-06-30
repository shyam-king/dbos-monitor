"""Process-global registry of ``workflow_name -> executor_type`` mappings.

Executors declare which workflows they own and the executor type that runs them. The
:class:`DBOSMonitorClient` snapshots this registry on every heartbeat and pushes it to the
monitor, which persists it (newest mapping wins). The monitor uses the mapping to re-home
workflows abandoned by executors it never tracked.

``register_workflow_type`` is a convenience decorator; ``register_workflow_mapping`` /
``set_workflow_mappings`` let you populate the same registry programmatically.
"""

_REGISTRY: dict[str, str] = {}


def register_workflow_mapping(workflow_name: str, executor_type: str) -> None:
	"""Map a single workflow name to the executor type that runs it (overwriting any prior)."""
	_REGISTRY[workflow_name] = executor_type


def set_workflow_mappings(mapping: dict[str, str]) -> None:
	"""Merge ``workflow_name -> executor_type`` entries into the registry, newest values winning."""
	_REGISTRY.update(mapping)


def get_workflow_mappings() -> dict[str, str]:
	"""Snapshot copy of the current registry, safe to send without exposing the live dict."""
	return dict(_REGISTRY)


def register_workflow_type(executor_type: str, name: str):
	"""Decorator recording that the decorated workflow runs on ``executor_type``.

	``name`` must match the name DBOS records for the workflow (i.e. the ``name`` passed to
	``@DBOS.workflow()``, or its ``func.__qualname__`` when none is given), since the monitor
	keys the mapping on that name.
	"""

	def decorator(func):
		register_workflow_mapping(name, executor_type)
		return func

	return decorator
