"""Async stress test with live dashboard.

Spawns many concurrent asyncio tasks that insert, transfer, query,
stream, and delete accounts.  Run with:

    python async_stress.py [--seconds 10] [--workers 4]
    python async_stress.py --backend postgres [--seconds 10]
"""

import asyncio
import random
import time
import uuid
from datetime import datetime
from uuid import UUID

import async_dbi as dbi

from common import BackendConfig, StressResult, build_backend_config, build_error_tracker, parse_stress_args
from common import rand_account_data

from dashboard import Dashboard, LiveMetrics

from dinao.backend import create_connection_pool

# -- async worker loop --------------------------------------------------


async def _run_worker(name, worker_id, duration, tracker, metrics, work):
    """Run an async work callable in a timed loop.

    :param name: display name (role) for metrics reporting
    :param worker_id: identifier for this worker
    :param duration: how many seconds to run
    :param tracker: shared ErrorTracker instance
    :param metrics: shared LiveMetrics instance
    :param work: async callable to invoke each iteration
    :returns: number of successful operations
    """
    deadline = time.monotonic() + duration
    ops = 0
    while time.monotonic() < deadline:
        try:
            await work()
            ops += 1
            metrics.record_op(name)
        except Exception as exc:
            tracker.record(exc)
        await asyncio.sleep(0)
    metrics.mark_done(name)
    return ops


# -- worker definitions -------------------------------------------------


async def inserter(worker_id, duration, tracker, metrics):
    """Bulk-insert batches of accounts until time runs out.

    :param worker_id: identifier for this worker
    :param duration: how many seconds to run
    :param tracker: shared ErrorTracker instance
    :param metrics: shared LiveMetrics instance
    :returns: number of successful operations
    """

    async def work():
        batch = [rand_account_data() for _ in range(20)]
        await dbi.bulk_create_accounts(batch)

    return await _run_worker("Inserter", worker_id, duration, tracker, metrics, work)


async def transferrer(worker_id, duration, tracker, metrics):
    """Pick two random accounts and transfer funds.

    :param worker_id: identifier for this worker
    :param duration: how many seconds to run
    :param tracker: shared ErrorTracker instance
    :param metrics: shared LiveMetrics instance
    :returns: number of successful operations
    """

    async def work():
        page = {"limit": 50, "offset": 0}
        accounts = await dbi.list_accounts(page)
        if len(accounts) < 2:
            return
        src, dst = random.sample(accounts, 2)
        amount = random.randint(1, max(1, src.balance // 4))
        await dbi.transfer(src.name, dst.name, amount)

    return await _run_worker("Transferrer", worker_id, duration, tracker, metrics, work)


async def reader(worker_id, duration, tracker, metrics):
    """Read a random page or stream a capped batch of accounts.

    :param worker_id: identifier for this worker
    :param duration: how many seconds to run
    :param tracker: shared ErrorTracker instance
    :param metrics: shared LiveMetrics instance
    :returns: number of successful operations
    """
    use_paginate = [True]

    async def work():
        total = await dbi.count_accounts()
        if use_paginate[0]:
            page_size = random.randint(5, 25)
            offset = random.randint(0, max(0, total - page_size))
            await dbi.list_accounts({"limit": page_size, "offset": offset})
        else:
            i = 0
            async for _ in dbi.stream_accounts():
                i += 1
                if i >= 50:
                    break
        use_paginate[0] = not use_paginate[0]

    return await _run_worker("Reader", worker_id, duration, tracker, metrics, work)


async def _check_native_single_types(acct):
    """Assert correct types from single-value queries for one account.

    Guards with ``is not None`` because the account may be deleted by
    another worker between the list query and the lookup.
    """
    name = await dbi.get_account_name(acct.account_id)
    if name is not None:
        assert isinstance(name, str), f"expected str, got {type(name)}"

    rate = await dbi.get_interest_rate(acct.account_id)
    if rate is not None:
        assert isinstance(rate, float), f"expected float, got {type(rate)}"

    ref = await dbi.get_account_ref(acct.account_id)
    if ref is not None:
        assert isinstance(ref, UUID), f"expected UUID, got {type(ref)}"

    created = await dbi.get_account_created(acct.account_id)
    if created is not None:
        assert isinstance(created, datetime), f"expected datetime, got {type(created)}"

    risk = await dbi.get_risk_score(acct.account_id)
    if risk is not None:
        assert isinstance(risk, complex), f"expected complex, got {type(risk)}"


async def checker(worker_id, duration, tracker, metrics):
    """Exercise all NATIVE_SINGLE types and transfer-audit queries.

    :param worker_id: identifier for this worker
    :param duration: how many seconds to run
    :param tracker: shared ErrorTracker instance
    :param metrics: shared LiveMetrics instance
    :returns: number of successful operations
    """

    async def work():
        page = {"limit": 10, "offset": 0}
        accounts = await dbi.list_accounts(page)
        if not accounts:
            return
        acct = random.choice(accounts)
        exists = await dbi.account_exists(f"no_such_{uuid.uuid4().hex}")
        assert exists is False, f"expected False, got {exists!r}"
        looked_up = await dbi.get_account(acct.account_id)
        if looked_up is not None:
            assert looked_up.name == acct.name
        missing = await dbi.get_account(999_999_999)
        assert missing is None, f"expected None, got {missing!r}"
        sent = await dbi.total_sent(acct.account_id)
        received = await dbi.total_received(acct.account_id)
        assert sent >= 0, f"sent={sent!r}"
        assert received >= 0, f"received={received!r}"
        await dbi.transfers_for(acct.account_id)
        [t async for t in dbi.stream_transfers_for(acct.account_id)]
        await _check_native_single_types(acct)

    return await _run_worker("Checker", worker_id, duration, tracker, metrics, work)


async def deleter(worker_id, duration, tracker, metrics):
    """Occasionally delete a random account.

    :param worker_id: identifier for this worker
    :param duration: how many seconds to run
    :param tracker: shared ErrorTracker instance
    :param metrics: shared LiveMetrics instance
    :returns: number of successful operations
    """

    async def work():
        page = {"limit": 100, "offset": 0}
        accounts = await dbi.list_accounts(page)
        if not accounts:
            return
        victim = random.choice(accounts)
        await dbi.delete_account(victim.account_id)

    return await _run_worker("Deleter", worker_id, duration, tracker, metrics, work)


# -- main ---------------------------------------------------------------

WORKER_FACTORIES = [
    ("Inserter", inserter),
    ("Transferrer", transferrer),
    ("Reader", reader),
    ("Checker", checker),
    ("Deleter", deleter),
]


async def run(config: BackendConfig, seconds: int, workers: int) -> StressResult:
    """Set up the pool, seed data, and fire concurrent tasks.

    :param config: backend configuration
    :param seconds: how long to run
    :param workers: number of workers per role
    :returns: aggregated stress test results
    """
    pool = create_connection_pool(config.async_url)
    dbi.binder.pool = pool
    dbi.ROW_LOCK = config.row_lock

    await dbi.init_schema(config.pk_col_type)
    seed = [rand_account_data() for _ in range(100)]
    await dbi.bulk_create_accounts(seed)

    role_names = [name for name, _ in WORKER_FACTORIES]
    metrics = LiveMetrics(role_names, workers, seconds)
    metrics.db_path = config.async_url

    dashboard = Dashboard(f"DINAO Async Stress Test ({config.name})", metrics)
    dashboard.console.print(f"[dim]Seeded {len(seed)} accounts into[/] [cyan]{config.async_url}[/]\n")

    tracker = build_error_tracker(on_error=metrics.record_error)

    tasks = []
    for i in range(workers):
        for _, factory in WORKER_FACTORIES:
            tasks.append(asyncio.create_task(factory(i, seconds, tracker, metrics)))

    dashboard.start()
    metrics.start()
    results = await asyncio.gather(*tasks)
    elapsed = metrics.elapsed

    total_ops = sum(results)
    final_count = await dbi.count_accounts()
    metrics.final_accounts = final_count
    dashboard.stop()

    result = StressResult(elapsed, total_ops, final_count, tracker)
    dashboard.print_summary(result)
    await pool.dispose()
    return result


def main():
    """Parse args and run the async stress test."""
    args = parse_stress_args("Async stress test")
    config = build_backend_config(args)
    asyncio.run(run(config, args.seconds, args.workers))


if __name__ == "__main__":
    main()
