import logging
import threading
import time

import httpx

logger = logging.getLogger(__name__)


def _default_dbos_recovery():
    from dbos._dbos import _get_dbos_instance
    from dbos._recovery import startup_recovery_thread
    from dbos._utils import GlobalParams

    dbos = _get_dbos_instance()
    if dbos is None:
        logger.warning("DBOS instance not available for recovery")
        return
    workflow_ids = dbos._sys_db.get_pending_workflows(
        GlobalParams.executor_id, GlobalParams.app_version
    )
    if workflow_ids:
        logger.info("Recovering %d workflows for executor %s", len(workflow_ids), GlobalParams.executor_id)
        dbos._executor.submit(startup_recovery_thread, dbos, workflow_ids)


class DBOSMonitorClient:
    def __init__(
        self,
        monitor_url: str,
        executor_id: str,
        executor_type: str,
        health_ping_interval_ms: int,
        on_recovery_needed=_default_dbos_recovery,
    ):
        self._monitor_url = monitor_url.rstrip("/")
        self._executor_id = executor_id
        self._executor_type = executor_type
        self._ping_interval_ms = health_ping_interval_ms
        self._on_recovery_needed = on_recovery_needed
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def _heartbeat_loop(self):
        interval_sec = self._ping_interval_ms / 1000
        while self._running:
            try:
                response = self._send_heartbeat()
                if response and response.get("recovery_needed"):
                    self._trigger_recovery()
            except Exception:
                logger.exception("Heartbeat failed")
            time.sleep(interval_sec)

    def _send_heartbeat(self) -> dict | None:
        try:
            resp = httpx.post(
                f"{self._monitor_url}/heartbeat",
                json={
                    "executor_id": self._executor_id,
                    "executor_type": self._executor_type,
                    "health_ping_interval_ms": self._ping_interval_ms,
                },
                timeout=5.0,
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError:
            logger.warning("Failed to send heartbeat to %s", self._monitor_url)
            return None

    def _trigger_recovery(self):
        logger.info("Recovery needed for executor %s", self._executor_id)
        if self._on_recovery_needed:
            self._on_recovery_needed()
