# Stress Test

Concurrent stress test for DINAO backends with a
live-updating terminal dashboard. Supports both SQLite
(default) and PostgreSQL. Exercises every major binding
feature (execute, query, transaction,
Optional/list/Generator/AsyncGenerator/bool/int/dict
returns) under heavy contention from multiple workers.
The checker worker validates all seven `NATIVE_SINGLE`
mapper types (`str`, `int`, `float`, `complex`, `bool`,
`datetime`, `UUID`) via dedicated single-value queries.

## DBI Layer

The database interface is organized under the `dbis/`
package with a dedicated module for each dialect and
execution mode:

```
dbis/
  __init__.py          # load_sync_dbi / load_async_dbi
  sqlite_sync.py       # sync SQLite DBI
  sqlite_async.py      # async SQLite DBI
  postgres_sync.py     # sync PostgreSQL DBI
  postgres_async.py    # async PostgreSQL DBI
```

Each module contains the full set of bound functions with
dialect-specific SQL baked in (e.g. `SERIAL PRIMARY KEY`
vs `INTEGER PRIMARY KEY AUTOINCREMENT`, `FOR UPDATE` row
locking for PostgreSQL). The stress runners call
`load_sync_dbi(backend)` or `load_async_dbi(backend)` to
obtain the appropriate module at startup.

## Setup

Create a virtual environment for this example:

```bash
cd examples/stress/
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e ../../          # install dinao from source
```

For PostgreSQL, start the database container:

```bash
docker compose up -d
```

## Running

Run from inside the `examples/stress/` directory with the
venv activated.

### SQLite (default, no docker needed)

```bash
# Sync (threaded) stress test
python sync_stress.py

# Async (aiosqlite) stress test
python async_stress.py
```

### PostgreSQL

```bash
# Sync (threaded) stress test
python sync_stress.py --backend postgres

# Async (psycopg async) stress test
python async_stress.py --backend postgres

# Async (asyncpg) stress test
python async_stress.py --backend postgres --engine asyncpg
```

### CLI Flags

| Flag          | Default  | Description                        |
|---------------|----------|------------------------------------|
| `--seconds`   | 10       | How long to run                    |
| `--workers`   | 3        | Number of workers per role         |
| `--backend`   | sqlite   | Backend: `sqlite` or `postgres`    |
| `--engine`    | psycopg  | Async engine: `psycopg`/`asyncpg` |
| `--url`       | (auto)   | Override connection URL            |
| `--fail-fast` | off      | Stop on first unexpected error     |

Each run spawns five worker roles (inserter, transferrer,
reader, checker, deleter) multiplied by the worker count.
For PostgreSQL the connection pool is automatically scaled
with `--workers` (pool_max = workers * 5 + 5, capped at
50) so every worker can obtain a connection without
queueing.

```bash
# Quick smoke test (SQLite)
python sync_stress.py --seconds 3 --workers 1

# Heavy contention (PostgreSQL, psycopg)
python async_stress.py --backend postgres --seconds 30 --workers 10

# Heavy contention (PostgreSQL, asyncpg)
python async_stress.py --backend postgres --engine asyncpg --seconds 30 --workers 10

# Custom connection URL
python sync_stress.py --url "sqlite3:///tmp/custom.db"
```

## Live Dashboard

While the test runs, a live-updating dashboard displays:

- **Header** with database URL, worker count, total ops,
  throughput, and an animated progress bar
- **Worker table** with per-role status, ops count, ops/s,
  and throughput bars color-coded by role
- **Error panel** with a scrolling log of recent errors,
  color-coded yellow for expected and red for unexpected

When the test finishes, the dashboard is replaced by a
static summary panel showing final results and a PASS/FAIL
verdict.

## Fail-Fast Mode

When `--fail-fast` is supplied, the test shuts down
immediately on the first unexpected error. A crash
report is written to the current directory as
`stress_crash_<datetime>.log` containing:

- Timestamp and Python version
- Backend, mode, URL, and worker configuration
- The full exception type, message, and traceback
- A summary of all expected and unexpected errors
  recorded up to the point of the crash

```bash
python async_stress.py --backend postgres --fail-fast
```

## Expected Errors

These errors are normal under concurrent write contention:

### SQLite
- **database_locked** -- `sqlite3.OperationalError` from
  SQLite write contention

### PostgreSQL
- **deadlock** -- `deadlock detected` from concurrent
  row-level locking

### Both backends
- **insufficient_funds** -- transfer rejected because the
  source account balance is too low
- **missing_account** -- account deleted by another worker
  between lookup and transfer

## Programmatic Use

Both `sync_stress.run()` and `async_stress.run()` return a
`StressResult` that can be used for automated assertions:

```python
from common import build_backend_config, parse_stress_args
from sync_stress import run

args = parse_stress_args("test")
config = build_backend_config(args)
result = run(config, seconds=3, workers=2)
assert result.errors.unexpected_count == 0
assert result.total_ops > 0
```
