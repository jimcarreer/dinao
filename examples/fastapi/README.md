# FastAPI DINAO Example

A simple example of how to use DINAO with FastAPI, using the async
engine (asyncpg) and Pydantic models. To run this example you need:

-   docker
-   docker-compose

Additionally you will want to install the python libraries `requests`
and `rich` to be able to run `tester.py`.

You simply need to run:

```
$ ./build.sh
$ docker compose up
$ pip install requests rich
$ python3 tester.py
```

## Migrations

This example uses the DINAO migration system to set up and seed the
database. Migration scripts live in `app/migrations/` and are
executed by `ini.py` via `AsyncMigrationRunner` before the
application starts.

The three included migrations:

1.  `20260216_001_create_data_table.py` -- Creates the `data` table.
2.  `20260216_002_seed_100_entries.py` -- Inserts 100 rows
    (`entry_001` through `entry_100`).
3.  `20260216_003_delete_last_three.py` -- Removes the last three
    seeded entries (`entry_098`, `entry_099`, `entry_100`).

Migrations are tracked in the `dinao_migration_revisions` table, so
they run only once even if the container is restarted.

## Special Notes

This example uses `AsyncFunctionBinder` with the `asyncpg` backend.
The `asyncpg` driver defaults to async mode, so no `+async` suffix
is needed in the connection URL:

    postgresql+asyncpg://user:pass@host:port/dbname

FastAPI is served via gunicorn using `uvicorn.workers.UvicornWorker`,
which provides an ASGI server capable of running async request
handlers. Unlike the Flask / WSGI example, async workers handle
concurrency within each process, so fewer worker processes are
needed.

The database pool is initialized using a FastAPI `lifespan` context
manager, which ensures proper setup on startup and cleanup on
shutdown.
