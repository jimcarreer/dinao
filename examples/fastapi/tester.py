"""Stress tester for the FastAPI DINAO example with a live dashboard."""

# pip install requests rich

import random
import signal
import threading
import time
import uuid

import requests

from dashboard import Dashboard, LiveMetrics

API_BASE = "http://localhost:5001"


class Smacker(threading.Thread):
    """Base class for API stress-test worker threads."""

    api_action_name = "unknown"

    def __init__(self, worker_num, metrics):
        """Construct a worker.

        :param worker_num: identifier for this worker
        :param metrics: shared ``LiveMetrics`` instance
        """
        super().__init__()
        self.shutdown_flag = threading.Event()
        self.api = f"{API_BASE}/items"
        self.worker_num = worker_num
        self.metrics = metrics

    def _api_action(self) -> requests.Response:
        """Execute the API call. Override in subclasses."""
        pass

    def run(self):
        """Run the API action in a loop until shutdown is signaled."""
        while not self.shutdown_flag.is_set():
            start_time = time.perf_counter()
            res = self._api_action()
            elapsed = time.perf_counter() - start_time
            self.metrics.record(self.api_action_name, elapsed, res.status_code)
        self.metrics.mark_worker_done()


class SmackPutter(Smacker):
    """Worker that posts new items."""

    api_action_name = "POST /items"

    def _api_action(self):
        """Create a new item via POST."""
        name = f"testing_{self.worker_num}_{str(uuid.uuid4())}"
        value = random.randint(1, 100)
        return requests.post(self.api, json={'name': name, 'value': value})


class SmackerLister(Smacker):
    """Worker that fetches paginated item lists."""

    api_action_name = "GET /items"

    def _api_action(self):
        """Fetch a random page of items."""
        params = {
            "page": random.randint(1, 5),
            "size": random.randint(1, 200),
            "search": f"testing_{self.worker_num}_%",
        }
        return requests.get(self.api, params=params)


class SmackerSummer(Smacker):
    """Worker that fetches summed item values."""

    api_action_name = "GET /summed"

    def _api_action(self):
        """Fetch summed values for a search prefix."""
        params = {
            "search": f"testing_{self.worker_num}%",
            "size": random.randint(1, 200),
        }
        return requests.get(f"{self.api}/summed", params=params)


class ShutdownNow(Exception):
    """Raised by the signal handler to trigger a clean shutdown."""

    @classmethod
    def raise_it(cls, signal_code, frame):
        """Signal handler that raises ShutdownNow."""
        raise cls


def main():
    """Run the stress test with a live dashboard."""
    signal.signal(signal.SIGTERM, ShutdownNow.raise_it)
    signal.signal(signal.SIGINT, ShutdownNow.raise_it)

    workers = 10
    action_names = ["POST /items", "GET /items", "GET /summed"]
    total_workers = workers * len(action_names)

    metrics = LiveMetrics(action_names, total_workers, f"{API_BASE}/items")
    dashboard = Dashboard(metrics)

    dashboard.wait_for_api(API_BASE)

    smackers = []
    try:
        smackers = [SmackerLister(w, metrics) for w in range(workers)]
        smackers += [SmackPutter(w, metrics) for w in range(workers)]
        smackers += [SmackerSummer(w, metrics) for w in range(workers)]

        dashboard.start()
        metrics.start()
        for smacker in smackers:
            smacker.start()
        while True:
            time.sleep(0.5)
    except ShutdownNow:
        for smacker in smackers:
            smacker.shutdown_flag.set()
        for smacker in smackers:
            smacker.join()
        dashboard.stop()
        dashboard.print_summary(metrics.snapshot())


if __name__ == "__main__":
    main()
