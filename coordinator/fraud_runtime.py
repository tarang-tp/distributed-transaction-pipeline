"""Drives one FraudWorker for real: a background thread pulling and scoring
records, with periodic group heartbeats. No internal locking needed here --
unlike RaftNodeRuntime, nothing external calls into a FraudWorker
concurrently with its own loop; the shared state it touches (ConsumerGroup,
PartitionedLog) already has its own locking (streaming/group.py,
streaming/broker.py).

stop() intentionally does NOT call the group's leave() -- it just kills the
thread, so the worker stops heartbeating and looks exactly like a crashed
process to the rest of the group. Reassignment only happens once something
calls group.check_expired_members(), which is the coordinator's job.
"""

from __future__ import annotations

import threading
import time

from fraud.worker import FraudWorker


class FraudWorkerRuntime:
    def __init__(self, worker: FraudWorker, poll_interval: float = 0.02, heartbeat_interval: float = 0.05):
        self.worker = worker
        self._poll_interval = poll_interval
        self._heartbeat_interval = heartbeat_interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def member_id(self) -> str:
        return self.worker.member_id

    def start(self) -> None:
        self.worker.join()
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name=f"fraud-{self.worker.member_id}", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _run(self) -> None:
        last_heartbeat = 0.0
        while not self._stop_event.is_set():
            now = time.monotonic()
            if now - last_heartbeat >= self._heartbeat_interval:
                self.worker.heartbeat()
                last_heartbeat = now
            produced = self.worker.run_once()
            if not produced:
                time.sleep(self._poll_interval)
