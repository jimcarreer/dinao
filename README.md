# DINAO Is Not An ORM

[![code-of-conduct](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](CODE_OF_CONDUCT.md)
[![build-status](https://github.com/jimcarreer/dinao/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/jimcarreer/dinao/actions/workflows/build.yml)
[![cover-status](https://codecov.io/gh/jimcarreer/dinao/branch/main/graph/badge.svg?token=CpJ5u1ngZH)](https://codecov.io/gh/jimcarreer/dinao)
[![pyver-status](https://img.shields.io/pypi/pyversions/dinao)](https://pypi.org/project/dinao/)
[![pypiv-status](https://badge.fury.io/py/dinao.svg?dummy)](https://pypi.org/project/dinao/)
[![coding-style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## Introduction

What is DINAO? Well it might be easier to tell you what it's not. DINAO
Is Not An ORM. If you want an ORM, [SQLAlchemy](https://sqlalchemy.org/)
is absolutely the best python has to offer.

### Target Audience

Do you like writing SQL? Do you hate all the boiler plate involved with
setting up connections and cursors then cleaning them up? Would you just
like something simple that executes a query and can map the results to
simple data classes? Then DINAO is for you!

### Influences and Guiding Principles

The APIs implemented mirror libraries I've used in other ecosystems.
Specifically, you may notice similarities to the JDBI Declarative API or
the MyBatis interface mappers. This is because I very much *like* this
approach. You're the developer, I'm just here to reduce the number of
lines of code you have to write to meet your goal. At the end of the day
you know your schema and database better than I do, and so you know what
kinds of queries you need to write better than I do.

### How do you pronounce DINAO?

You pronounce it "Dino" like "Dinosaur". Going back to plain old SQL
probably seems rather archaic after all.

## Usage

Install via pip:

```
$ pip install dinao
```

You will also need to install your backend driver. Backends + drivers
supported are:

- SQLite3 via Python's standard library
- PostgreSQL via [psycopg2](https://pypi.org/project/psycopg2/) or
  [psycopg (v3)](https://pypi.org/project/psycopg/)
- MariaDB via [mariadb connector](https://pypi.org/project/mariadb/)
- MySQL via
  [mysql-connector-python](https://pypi.org/project/mysql-connector-python/)

For detailed connection string formats and examples see the [backends
documentation](BACKENDS.md).

### Basic Example

DINAO focuses binding functions to scoped connections / transactions
against the database and using function signatures and type hinting to
infer mapping and query parameterization.

Below shows a simple example of DINAO usage with async PostgreSQL. For
more comprehensive usage and feature showcase see
[examples](https://github.com/jimcarreer/dinao/tree/main/examples).

``` python
import asyncio
from typing import List
from dataclasses import dataclass
from dinao.backend import create_connection_pool, AsyncConnection
from dinao.binding import AsyncFunctionBinder

binder = AsyncFunctionBinder()

@dataclass
class MyModel:
    name: str
    value: int

@binder.execute(
    "CREATE TABLE IF NOT EXISTS my_table ( "
    "  name VARCHAR(32) PRIMARY KEY, "
    "  value INTEGER DEFAULT 0"
    ")"
)
async def make_table():
    pass


@binder.execute(
    "INSERT INTO my_table (name, value) "
    "VALUES(#{model.name}, #{model.value}) "
    "ON CONFLICT (name) DO UPDATE "
    "  SET value = #{model.value} "
    "WHERE my_table.name = #{model.name}"
)
async def upsert(model: MyModel) -> int:
    pass


# This is an example of a query where a template variable is
# directly replaced in a template.  This is via a template
# argument denoted with !{column_name}.  The #{search_term} on
# the other hand uses proper escaping and parameterization in the
# underlying SQL engine.
#
# IMPORTANT: This is a vector for SQL Injection, do not use
#            direct template replacement on untrusted inputs,
#            especially those coming from users.  Ensure that you
#            validate, restrict, or otherwise limit the values
#            that can be used in direct template replacement.
#
@binder.query(
    "SELECT name, value FROM my_table "
    "WHERE !{column_name} LIKE #{search_term}"
)
async def search(
    column_name: str, search_term: str
) -> List[MyModel]:
    pass


@binder.transaction()
async def populate(cnx: AsyncConnection = None):
    await make_table()
    await cnx.commit()
    await upsert(MyModel("testing", 52))
    await upsert(MyModel("test", 39))
    await upsert(MyModel("other_thing", 20))


async def main():
    con_url = (
        "postgresql+psycopg+async://"
        "user:pass@localhost:5432/mydb"
    )
    db_pool = create_connection_pool(con_url)
    binder.pool = db_pool
    await populate()
    for model in await search("name", "test%"):
        print(f"{model.name}: {model.value}")
    await db_pool.dispose()


if __name__ == '__main__':
    asyncio.run(main())
```

### Migrations

DINAO includes a simple, forward-only migration system for managing
schema changes. Migration scripts are plain Python files that define
an `upgrade` function. The runner discovers scripts in a directory,
tracks which have been applied, and executes pending ones in order.
A locking mechanism prevents concurrent migration runs.

**Script naming**: By default, files must match a date and sequence
pattern such as `20260216_001_create_users.py`. This can be changed
by passing a custom regex to the runner's `pattern` parameter.
Scripts are applied in lexicographic order.

Each script defines an `upgrade(cnx)` function that receives a
`MigrationConnection` (sync) or `AsyncMigrationConnection` (async).
The connection provides `execute()` and `query()` methods that
support the DINAO `#{var}` template syntax:

``` python
# migrations/20260216_001_create_users.py
"""Create the users table."""


async def upgrade(cnx):
    """Create users table with name and email columns."""
    await cnx.execute(
        "CREATE TABLE IF NOT EXISTS users ("
        "  name VARCHAR(256) PRIMARY KEY,"
        "  email VARCHAR(256) NOT NULL"
        ")"
    )
```

``` python
# migrations/20260216_002_seed_data.py
"""Insert initial user records."""


async def upgrade(cnx):
    """Seed the users table with default entries."""
    for name, email in [("admin", "admin@co.com"), ("bob", "bob@co.com")]:
        await cnx.execute(
            "INSERT INTO users (name, email) VALUES (#{name}, #{email})",
            name=name,
            email=email,
        )
```

To run migrations, create a `MigrationRunner` (sync) or
`AsyncMigrationRunner` (async) and call `upgrade()`:

``` python
import asyncio
import os
from dinao.migration import AsyncMigrationRunner

db_url = "postgresql+asyncpg://user:pass@localhost:5432/mydb"
script_dir = os.path.join(os.path.dirname(__file__), "migrations")

runner = AsyncMigrationRunner(db_url, script_dir)
asyncio.run(runner.upgrade())
```

The runner automatically creates two tracking tables
(`dinao_migration_revisions` and `dinao_migration_state`) to record
applied revisions and migration run status. Each script runs in its
own transaction -- if a script fails, the error is recorded and a
`RevisionError` is raised.

For a complete working example integrating migrations with
FastAPI, see
[examples/fastapi](https://github.com/jimcarreer/dinao/tree/main/examples/fastapi).

## Contributing

Check out our [code of
conduct](https://github.com/jimcarreer/dinao/blob/main/CODE_OF_CONDUCT.md)
and [contributing
documentation](https://github.com/jimcarreer/dinao/blob/main/CONTRIBUTING.md).

## Release Process

This library adheres too [semantic versioning
2.0.0](https://semver.org/spec/v2.0.0.html) standards, in general that
means, given a version number MAJOR.MINOR.PATCH, increment:

1. MAJOR version when you make incompatible API changes
2. MINOR version when you add functionality in a backwards compatible
   manner
3. PATCH version when you make backwards compatible bug fixes

### Preview Builds

Every merge to main automatically publishes a development preview to
PyPI, provided that `__version__.py` contains a `.dev` suffix. These
builds use [PEP 440](https://peps.python.org/pep-0440/) development
release versions (e.g. `2.1.0.dev42`).

To install the latest preview:

```
$ pip install --pre dinao
```

Preview builds are **not** installed by default; `pip install dinao`
will always resolve to the latest stable release.

### Stable Releases

Changes for the next version accumulate on the main branch until there
is enough confidence in the build that it can be released. The release
workflow is:

1. A repository administrator opens a PR to set `__version__.py` to
   the release version (e.g. `2.1.0`), and updates the change logs
2. The PR is merged to main and the merge commit is tagged with the
   release version (e.g. `release/2.1.0`)
3. Only tagged commits of main are built and published as stable
   releases
4. Immediately after tagging, a follow-up PR bumps `__version__.py`
   to the next anticipated version with a `.dev0` suffix (e.g.
   `2.2.0.dev0`) so that preview builds resume
