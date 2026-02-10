"""Sync stress test with live dashboard.

Spawns multiple threads that concurrently insert, transfer, query,
stream, and delete accounts.  Run with:

    python sync_stress.py [--seconds 10] [--workers 4]
    python sync_stress.py --backend postgres [--seconds 10]
"""

import random
import threading
import time
import uuid
from datetime import datetime
from uuid import UUID

from common import (
    BackendConfig,
    StressResult,
    build_backend_config,
    build_error_tracker,
    parse_stress_args,
    rand_account_data,
)

from dashboard import Dashboard, LiveMetrics

from dinao.backend import create_connection_pool

import sync_dbi as dbi

# -- worker base --------------------------------------------------------


class Worker(threading.Thread):
    """Base class for stress-test worker threads."""

    def __init__(self, worker_id, duration, tracker, metrics):
        """Construct a worker.

        :param worker_id: identifier for this worker
        :param duration: how many seconds to run
        :param tracker: shared ErrorTracker instance
        :param metrics: shared LiveMetrics instance
        """
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.duration = duration
        self.tracker = tracker
        self.metrics = metrics
        self.ops = 0
        self._role = self.__class__.__name__

    def work(self):
        """Override in subclasses to do one unit of work."""
        raise NotImplementedError

    def run(self):
        """Run work() in a loop until the duration elapses or shutdown is signalled."""
        deadline = time.monotonic() + self.duration
        while time.monotonic() < deadline and not self.tracker.shutdown.is_set():
            try:
                self.work()
                self.ops += 1
                self.metrics.record_op(self._role)
            except Exception as exc:
                self.tracker.record(exc)
        self.metrics.mark_done(self._role)


# -- workers ------------------------------------------------------------


class Inserter(Worker):
    """Bulk-inserts batches of accounts."""

    def work(self):
        """Insert a batch of accounts."""
        batch = [rand_account_data() for _ in range(20)]
        dbi.bulk_create_accounts(batch)


class Transferrer(Worker):
    """Picks two random accounts and transfers funds."""

    def work(self):
        """Transfer between two random accounts."""
        page = {"limit": 50, "offset": 0}
        accounts = dbi.list_accounts(page)
        if len(accounts) < 2:
            return
        src, dst = random.sample(accounts, 2)
        amount = random.randint(1, max(1, src.balance // 4))
        dbi.transfer(src.name, dst.name, amount)


class Reader(Worker):
    """Reads accounts via paginated queries and streaming."""

    def work(self):
        """Read a random page or stream a capped batch of accounts."""
        total = dbi.count_accounts()
        if self.ops % 2 == 0:
            page_size = random.randint(5, 25)
            offset = random.randint(0, max(0, total - page_size))
            dbi.list_accounts({"limit": page_size, "offset": offset})
        else:
            for i, _ in enumerate(dbi.stream_accounts()):
                if i >= 49:
                    break


def _check_native_single_types(acct):
    """Assert correct types from single-value queries for one account.

    Guards with ``is not None`` because the account may be deleted by
    another worker between the list query and the lookup.
    """
    name = dbi.get_account_name(acct.account_id)
    if name is not None:
        assert isinstance(name, str), f"expected str, got {type(name)}"

    rate = dbi.get_interest_rate(acct.account_id)
    if rate is not None:
        assert isinstance(rate, float), f"expected float, got {type(rate)}"

    ref = dbi.get_account_ref(acct.account_id)
    if ref is not None:
        assert isinstance(ref, UUID), f"expected UUID, got {type(ref)}"

    created = dbi.get_account_created(acct.account_id)
    if created is not None:
        assert isinstance(created, datetime), f"expected datetime, got {type(created)}"

    risk = dbi.get_risk_score(acct.account_id)
    if risk is not None:
        assert isinstance(risk, complex), f"expected complex, got {type(risk)}"


class Checker(Worker):
    """Exercises all NATIVE_SINGLE types and transfer-audit queries."""

    def work(self):
        """Run various query shapes covering every NATIVE_SINGLE type."""
        page = {"limit": 10, "offset": 0}
        accounts = dbi.list_accounts(page)
        if not accounts:
            return
        acct = random.choice(accounts)
        exists = dbi.account_exists(f"no_such_{uuid.uuid4().hex}")
        assert exists is False, f"expected False, got {exists!r}"
        looked_up = dbi.get_account(acct.account_id)
        if looked_up is not None:
            assert looked_up.name == acct.name
        missing = dbi.get_account(999_999_999)
        assert missing is None, f"expected None, got {missing!r}"
        sent = dbi.total_sent(acct.account_id)
        received = dbi.total_received(acct.account_id)
        assert sent >= 0, f"sent={sent!r}"
        assert received >= 0, f"received={received!r}"
        dbi.transfers_for(acct.account_id)
        for _ in dbi.stream_transfers_for(acct.account_id):
            pass
        _check_native_single_types(acct)


class Deleter(Worker):
    """Occasionally deletes a random account."""

    def work(self):
        """Delete one random account."""
        page = {"limit": 100, "offset": 0}
        accounts = dbi.list_accounts(page)
        if not accounts:
            time.sleep(0.01)
            return
        victim = random.choice(accounts)
        dbi.delete_account(victim.account_id)


# -- main ---------------------------------------------------------------

WORKER_CLASSES = [Inserter, Transferrer, Reader, Checker, Deleter]


def run(config: BackendConfig, seconds: int, workers: int, fail_fast: bool = False) -> StressResult:
    """Set up the pool, seed data, and fire concurrent workers.

    :param config: backend configuration
    :param seconds: how long to run
    :param workers: number of workers per role
    :param fail_fast: if True, stop on first unexpected error
    :returns: aggregated stress test results
    """
    pool = create_connection_pool(config.sync_url)
    dbi.binder.pool = pool
    dbi.ROW_LOCK = config.row_lock

    dbi.init_schema(config.pk_col_type)
    seed = [rand_account_data() for _ in range(100)]
    dbi.bulk_create_accounts(seed)

    role_names = [cls.__name__ for cls in WORKER_CLASSES]
    metrics = LiveMetrics(role_names, workers, seconds)
    metrics.db_path = config.sync_url

    dashboard = Dashboard(f"DINAO Sync Stress Test ({config.name})", metrics)
    dashboard.console.print(f"[dim]Seeded {len(seed)} accounts into[/] [cyan]{config.sync_url}[/]\n")

    tracker = build_error_tracker(on_error=metrics.record_error, fail_fast=fail_fast)
    tracker.context = {
        "Mode": "sync",
        "Backend": config.name,
        "URL": config.sync_url,
        "Workers per role": str(workers),
        "Planned duration": f"{seconds}s",
    }

    threads = []
    for i in range(workers):
        for cls in WORKER_CLASSES:
            threads.append(cls(i, seconds, tracker, metrics))

    dashboard.start()
    metrics.start()
    for w in threads:
        w.start()
    for w in threads:
        w.join()
    elapsed = metrics.elapsed

    total_ops = sum(w.ops for w in threads)
    final_count = dbi.count_accounts()
    metrics.final_accounts = final_count
    dashboard.stop()

    result = StressResult(elapsed, total_ops, final_count, tracker)
    dashboard.print_summary(result)
    if tracker.crash_report_path:
        dashboard.console.print(
            f"[bold red]Fail-fast triggered. Crash report: {tracker.crash_report_path}[/]"
        )
    pool.dispose()
    return result


def main():
    """Parse args and run the sync stress test."""
    args = parse_stress_args("Sync stress test")
    config = build_backend_config(args)
    run(config, args.seconds, args.workers, fail_fast=args.fail_fast)


if __name__ == "__main__":
    main()
