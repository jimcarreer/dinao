# pip install requests

import uuid
import threading
import signal
import time
import random
import statistics

import requests


class ResponseStats:
    """Thread-safe collector for response time statistics."""

    def __init__(self):
        self._lock = threading.Lock()
        self._by_action = {}  # action_name -> list of response times
        self._all_times = []  # all response times
        self._status_codes = {}  # status_code -> count
        self._status_by_action = {}  # action_name -> {status_code -> count}

    def record(self, action_name: str, response_time: float, status_code: int):
        with self._lock:
            if action_name not in self._by_action:
                self._by_action[action_name] = []
                self._status_by_action[action_name] = {}
            self._by_action[action_name].append(response_time)
            self._all_times.append(response_time)
            # Track status codes globally
            self._status_codes[status_code] = self._status_codes.get(status_code, 0) + 1
            # Track status codes per action
            action_statuses = self._status_by_action[action_name]
            action_statuses[status_code] = action_statuses.get(status_code, 0) + 1

    def _calc_stats(self, times_ms: list) -> dict:
        """Calculate statistics for a list of times in milliseconds."""
        if not times_ms:
            return None
        return {
            "count": len(times_ms),
            "min": min(times_ms),
            "max": max(times_ms),
            "mean": statistics.mean(times_ms),
            "median": statistics.median(times_ms),
        }

    def summarize(self):
        """Print a summary table of response time statistics."""
        with self._lock:
            if not self._all_times:
                print("\nNo API calls recorded.")
                return

            # Convert all times to milliseconds
            all_times_ms = [t * 1000 for t in self._all_times]
            global_stats = self._calc_stats(all_times_ms)

            # Calculate success/error counts
            total_calls = sum(self._status_codes.values())
            success_count = sum(cnt for code, cnt in self._status_codes.items() if 200 <= code < 300)
            error_count = total_calls - success_count

            # Header
            print("\n" + "=" * 78)
            print("RESPONSE TIME STATISTICS (milliseconds)")
            print("=" * 78)

            # Global stats
            print("\n--- ALL REQUESTS ---")
            print(f"  Total Calls: {global_stats['count']}")
            print(f"  Min:         {global_stats['min']:.2f} ms")
            print(f"  Max:         {global_stats['max']:.2f} ms")
            print(f"  Mean:        {global_stats['mean']:.2f} ms")
            print(f"  Median:      {global_stats['median']:.2f} ms")

            # Status code summary
            print("\n--- STATUS CODES ---")
            print(f"  Successful (2xx): {success_count}")
            print(f"  Errors (non-2xx): {error_count}")
            if self._status_codes:
                print("\n  Breakdown by status code:")
                for code in sorted(self._status_codes.keys()):
                    count = self._status_codes[code]
                    pct = (count / total_calls) * 100
                    print(f"    {code}: {count} ({pct:.1f}%)")

            # Per-action stats table
            print("\n--- BY API ACTION ---")
            print(f"{'Action':<20} {'Count':>10} {'Min':>10} {'Max':>10} {'Mean':>10} {'Median':>10}")
            print("-" * 78)

            for action_name in sorted(self._by_action.keys()):
                times_ms = [t * 1000 for t in self._by_action[action_name]]
                s = self._calc_stats(times_ms)
                print(f"{action_name:<20} {s['count']:>10} {s['min']:>10.2f} {s['max']:>10.2f} {s['mean']:>10.2f} {s['median']:>10.2f}")

            # Per-action status codes
            print("\n--- STATUS CODES BY ACTION ---")
            for action_name in sorted(self._status_by_action.keys()):
                action_statuses = self._status_by_action[action_name]
                action_total = sum(action_statuses.values())
                action_success = sum(cnt for code, cnt in action_statuses.items() if 200 <= code < 300)
                action_errors = action_total - action_success
                print(f"  {action_name}:")
                print(f"    Successful: {action_success}, Errors: {action_errors}")
                for code in sorted(action_statuses.keys()):
                    count = action_statuses[code]
                    print(f"      {code}: {count}")

            print("=" * 78)


# Global stats collector
stats = ResponseStats()


class Smacker(threading.Thread):

    api_action_name = "unknown"

    def __init__(self, worker_num: int):
        super().__init__()
        self.shutdown_flag = threading.Event()
        self.api = "http://localhost:5001/items"
        self.worker_num = worker_num

    def _api_action(self) -> requests.Response:
        pass

    def run(self):
        tid = threading.get_ident()
        cnt = 0
        print(f"{tid}: Starting")
        while not self.shutdown_flag.is_set():
            cnt += 1
            start_time = time.perf_counter()
            res = self._api_action()
            elapsed = time.perf_counter() - start_time
            stats.record(self.api_action_name, elapsed, res.status_code)
            if res.status_code != 200:
                print(f"{tid}: Got non 200: {res.status_code} {res.text}")
            if cnt % 100 == 0:
                print(f"{tid}: Made {cnt} calls")
        print(f"{tid}: Shutting down")


class SmackPutter(Smacker):

    api_action_name = "POST /items"

    def _api_action(self):
        name = f"testing_{self.worker_num}_{str(uuid.uuid4())}"
        value = random.randint(1, 100)
        return requests.post(self.api, json={'name': name, 'value': value})


class SmackerLister(Smacker):

    api_action_name = "GET /items"

    def _api_action(self):
        params = {
            "page": random.randint(1, 5),
            "size": random.randint(1, 200),
            "search": f"testing_{self.worker_num}_%",
        }
        return requests.get(self.api, params=params)


class SmackerSummer(Smacker):

    api_action_name = "GET /summed"

    def _api_action(self):
        params = {
            "search": f"testing_{self.worker_num}%",
            "size": random.randint(1, 200),
        }
        return requests.get(f"{self.api}/summed", params=params)


class ShutdownNow(Exception):

    @classmethod
    def raise_it(cls, signal_code, frame):
        raise cls


def main():
    signal.signal(signal.SIGTERM, ShutdownNow.raise_it)
    signal.signal(signal.SIGINT, ShutdownNow.raise_it)
    workers = 10
    smackers = []
    try:
        smackers = [SmackerLister(w) for w in range(workers)]
        smackers += [SmackPutter(w) for w in range(workers)]
        smackers += [SmackerSummer(w) for w in range(workers)]
        for smacker in smackers:
            smacker.start()
        while True:
            time.sleep(0.5)
    except ShutdownNow:
        for smacker in smackers:
            smacker.shutdown_flag.set()
            smacker.join()
        stats.summarize()


if __name__ == "__main__":
    main()
