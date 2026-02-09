# Supported Backends

DINAO uses connection URLs to configure database backends. The general
format is:

    {backend}+{driver}://{user}:{password}@{host}:{port}/{db_name}

Where `{backend}` identifies the database type and `{driver}` identifies
the Python library used to connect. If the driver is omitted, a default
driver is used (where applicable). Additional options can be appended as
query string parameters.

Async mode can be enabled by appending `+async` to the driver in the URL
scheme:

    {backend}+{driver}+async://{user}:{password}@{host}:{port}/{db_name}

See the [Async Usage](#async-usage) section below for details on how to
use async backends with `AsyncFunctionBinder`.

## Support Matrix

| Backend    | Engine (driver)        | Sync | Async | Default Mode |
|------------|------------------------|------|-------|--------------|
| SQLite3    | `sqlite3` (stdlib)     | Yes  | No    | sync         |
| SQLite3    | `aiosqlite`            | No   | Yes   | async        |
| PostgreSQL | `psycopg2`             | Yes  | No    | sync         |
| PostgreSQL | `psycopg` (v3)         | Yes  | Yes   | sync         |
| PostgreSQL | `asyncpg`              | No   | Yes   | async        |
| MariaDB    | `mariadbconnector`     | Yes  | No    | sync         |
| MySQL      | `mysqlconnector`       | Yes  | No    | sync         |

## SQLite3

**Driver:** Python standard library (`sqlite3`)
**Install:** No additional packages required

SQLite3 is supported out of the box via Python's standard library.
Because SQLite3 operates on local files, the URL format is simpler than
the other backends.

### URL Format

    sqlite3://{file_path}

The `{file_path}` is the path to the SQLite3 database file on disk.

> **Note:** The path is resolved to an absolute path via
> `os.path.abspath`. Tilde (`~`) home directory expansion is also
> supported.

### Examples

``` python
from dinao.backend import create_connection_pool

# Absolute path
pool = create_connection_pool(
    "sqlite3:///var/data/myapp.db"
)

# Relative path
pool = create_connection_pool(
    "sqlite3://myapp.db"
)

# Home directory path
pool = create_connection_pool(
    "sqlite3://~/data/myapp.db"
)

# Temporary or in-memory style usage
pool = create_connection_pool(
    "sqlite3:///tmp/scratch.db"
)
```

### aiosqlite

**Driver:** [aiosqlite](https://pypi.org/project/aiosqlite/)
**Install:** `pip install aiosqlite`
**Async:** Yes (async only, defaults to async mode)

> **Note:** aiosqlite is an async-only driver. It does not support
> synchronous mode. The `+async` suffix is optional because aiosqlite
> defaults to async mode automatically. Explicitly specifying `+sync`
> will raise an `UnsupportedBackendError`.

#### URL Format

    sqlite3+aiosqlite://{file_path}

The explicit `+async` form is also accepted:

    sqlite3+aiosqlite+async://{file_path}

#### Examples

``` python
from dinao.backend import create_connection_pool

# Basic asynchronous connection
pool = create_connection_pool(
    "sqlite3+aiosqlite:///var/data/myapp.db"
)

# Explicit +async (also valid)
pool = create_connection_pool(
    "sqlite3+aiosqlite+async:///var/data/myapp.db"
)

# Home directory path
pool = create_connection_pool(
    "sqlite3+aiosqlite://~/data/myapp.db"
)
```

## PostgreSQL

DINAO supports PostgreSQL via three drivers: `psycopg2`, `psycopg`
(version 3), and `asyncpg`.

### Common Optional Arguments

All PostgreSQL drivers support the following query string parameters:

- `schema` - a list of schema names that sets the search path for
  connections, defaults to `public`
- `pool_min_conn` - an integer specifying the minimum number of
  connections in the pool, defaults to `1`
- `pool_max_conn` - an integer specifying the maximum number of
  connections in the pool, defaults to the value of `pool_min_conn`
- `sslmode` - a string (e.g. `prefer`, `verify-full`) specifying the
  SSL mode for connections
- `sslrootcert` - a path to a root CA certificate for SSL verification

### psycopg2

**Driver:** [psycopg2](https://pypi.org/project/psycopg2/)
**Install:** `pip install psycopg2` or `pip install psycopg2-binary`
**Default:** Yes (`postgresql://` without a driver defaults to
`psycopg2`)

#### URL Format

    postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}?{options}

Since `psycopg2` is the default driver for PostgreSQL, the `+psycopg2`
portion may be omitted:

    postgresql://{user}:{password}@{host}:{port}/{db_name}?{options}

In addition to the common PostgreSQL optional arguments, psycopg2
supports:

- `pool_threaded` - a boolean (`true` or `false`) specifying that a
  threaded connection pool should be used, defaults to `false`

#### Examples

``` python
from dinao.backend import create_connection_pool

# Basic connection (uses default psycopg2 driver)
pool = create_connection_pool(
    "postgresql://myuser:mypass@localhost:5432/mydb"
)

# Explicit driver specification
pool = create_connection_pool(
    "postgresql+psycopg2://myuser:mypass@localhost:5432/mydb"
)

# With connection pool sizing
pool = create_connection_pool(
    "postgresql://myuser:mypass@localhost:5432/mydb"
    "?pool_min_conn=2&pool_max_conn=10"
)

# With a threaded pool
pool = create_connection_pool(
    "postgresql://myuser:mypass@localhost:5432/mydb"
    "?pool_threaded=true&pool_min_conn=5&pool_max_conn=20"
)

# With SSL and a custom schema
pool = create_connection_pool(
    "postgresql://myuser:mypass@db.example.com:5432/mydb"
    "?sslmode=verify-full"
    "&sslrootcert=/etc/ssl/certs/db-ca.pem"
    "&schema=app_schema"
)
```

### psycopg (v3)

**Driver:** [psycopg](https://pypi.org/project/psycopg/) and
[psycopg-pool](https://pypi.org/project/psycopg-pool/)
**Install:** `pip install psycopg psycopg-pool`
**Async:** Yes (append `+async` to the driver)

#### URL Format

Synchronous:

    postgresql+psycopg://{user}:{password}@{host}:{port}/{db_name}?{options}

Asynchronous:

    postgresql+psycopg+async://{user}:{password}@{host}:{port}/{db_name}?{options}

The psycopg (v3) driver supports the common PostgreSQL optional
arguments listed above. Unlike psycopg2, this driver does not have a
`pool_threaded` option.

The async variant returns an `AsyncConnectionPool` which is required by
`AsyncFunctionBinder`. See the [Async Usage](#async-usage) section for a
full example.

#### Examples

``` python
from dinao.backend import create_connection_pool

# Basic synchronous connection
pool = create_connection_pool(
    "postgresql+psycopg://myuser:mypass@localhost:5432/mydb"
)

# Basic asynchronous connection
pool = create_connection_pool(
    "postgresql+psycopg+async://myuser:mypass"
    "@localhost:5432/mydb"
)

# Async with connection pool sizing
pool = create_connection_pool(
    "postgresql+psycopg+async://myuser:mypass"
    "@localhost:5432/mydb"
    "?pool_min_conn=2&pool_max_conn=10"
)

# With SSL
pool = create_connection_pool(
    "postgresql+psycopg://myuser:mypass@db.example.com:5432"
    "/mydb?sslmode=verify-full"
    "&sslrootcert=/etc/ssl/certs/db-ca.pem"
)
```

### asyncpg

**Driver:** [asyncpg](https://pypi.org/project/asyncpg/)
**Install:** `pip install asyncpg`
**Async:** Yes (async only, defaults to async mode)

> **Note:** asyncpg is an async-only driver. It does not support
> synchronous mode. The `+async` suffix is optional because asyncpg
> defaults to async mode automatically. Explicitly specifying `+sync`
> will raise an `UnsupportedBackendError`.

#### URL Format

    postgresql+asyncpg://{user}:{password}@{host}:{port}/{db_name}?{options}

The explicit `+async` form is also accepted:

    postgresql+asyncpg+async://{user}:{password}@{host}:{port}/{db_name}?{options}

The asyncpg driver supports the common PostgreSQL optional arguments
listed above.

#### Examples

``` python
from dinao.backend import create_connection_pool

# Basic asynchronous connection
pool = create_connection_pool(
    "postgresql+asyncpg://myuser:mypass"
    "@localhost:5432/mydb"
)

# Explicit +async (also valid)
pool = create_connection_pool(
    "postgresql+asyncpg+async://myuser:mypass"
    "@localhost:5432/mydb"
)

# Async with connection pool sizing
pool = create_connection_pool(
    "postgresql+asyncpg://myuser:mypass"
    "@localhost:5432/mydb"
    "?pool_min_conn=2&pool_max_conn=10"
)

# With SSL
pool = create_connection_pool(
    "postgresql+asyncpg://myuser:mypass"
    "@db.example.com:5432/mydb"
    "?sslmode=verify-full"
    "&sslrootcert=/etc/ssl/certs/db-ca.pem"
)
```

## MariaDB

**Driver:** [mariadb](https://pypi.org/project/mariadb/) (MariaDB
Connector/Python)
**Install:** `pip install mariadb`
**Default:** Yes (`mariadb://` without a driver defaults to
`mariadbconnector`)

### URL Format

    mariadb+mariadbconnector://{user}:{password}@{host}:{port}/{db_name}?{options}

Since `mariadbconnector` is the default driver, the `+mariadbconnector`
portion may be omitted:

    mariadb://{user}:{password}@{host}:{port}/{db_name}?{options}

### Optional Arguments

- `pool_name` - a string specifying a unique name for the connection
  pool, defaults to a random string
- `pool_size` - an integer specifying the size of the connection pool,
  defaults to `5`
- `ssl` - a boolean (`true` or `false`) specifying that the connection
  must use encryption
- `ssl_ca` - an absolute path to a CA certificate for server
  verification
- `ssl_verify_cert` - a boolean specifying that the server certificate
  must be verified

### Examples

``` python
from dinao.backend import create_connection_pool

# Basic connection (uses default mariadbconnector driver)
pool = create_connection_pool(
    "mariadb://myuser:mypass@localhost:3306/mydb"
)

# Explicit driver specification
pool = create_connection_pool(
    "mariadb+mariadbconnector://myuser:mypass"
    "@localhost:3306/mydb"
)

# With a named pool
pool = create_connection_pool(
    "mariadb://myuser:mypass@localhost:3306/mydb"
    "?pool_name=myapp-pool&pool_size=10"
)

# With SSL
pool = create_connection_pool(
    "mariadb://myuser:mypass@db.example.com:3306/mydb"
    "?ssl=true"
    "&ssl_ca=/etc/ssl/certs/db-ca.pem"
    "&ssl_verify_cert=true"
)
```

## MySQL

**Driver:**
[mysql-connector-python](https://pypi.org/project/mysql-connector-python/)
**Install:** `pip install mysql-connector-python`
**Default:** Yes (`mysql://` without a driver defaults to
`mysqlconnector`)

### URL Format

    mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db_name}?{options}

Since `mysqlconnector` is the default driver, the `+mysqlconnector`
portion may be omitted:

    mysql://{user}:{password}@{host}:{port}/{db_name}?{options}

### Optional Arguments

- `pool_name` - a string specifying a unique name for the connection
  pool, defaults to a random string
- `pool_size` - an integer specifying the size of the connection pool,
  defaults to `5`
- `ssl_ca` - an absolute path to a CA certificate for server
  verification
- `ssl_verify_cert` - a boolean specifying that the server certificate
  must be verified

### Examples

``` python
from dinao.backend import create_connection_pool

# Basic connection (uses default mysqlconnector driver)
pool = create_connection_pool(
    "mysql://myuser:mypass@localhost:3306/mydb"
)

# Explicit driver specification
pool = create_connection_pool(
    "mysql+mysqlconnector://myuser:mypass"
    "@localhost:3306/mydb"
)

# With a named pool
pool = create_connection_pool(
    "mysql://myuser:mypass@localhost:3306/mydb"
    "?pool_name=myapp-pool&pool_size=10"
)

# With SSL
pool = create_connection_pool(
    "mysql://myuser:mypass@db.example.com:3306/mydb"
    "?ssl_ca=/etc/ssl/certs/db-ca.pem"
    "&ssl_verify_cert=true"
)
```

## Async Usage

DINAO supports asynchronous database access via `AsyncFunctionBinder`.
Async support requires a backend that provides an `AsyncConnectionPool`;
the PostgreSQL `psycopg` (v3) and `asyncpg` drivers, and the SQLite3
`aiosqlite` driver support this (see the
[Support Matrix](#support-matrix) above).

Async mode is enabled by appending `+async` to the driver portion of the
connection URL. `AsyncFunctionBinder` uses `contextvars.ContextVar`
(instead of `threading.local`) for connection storage, making it safe
for use with `asyncio` and frameworks such as FastAPI, Starlette, and
aiohttp.

### Setting Up

Create an `AsyncFunctionBinder` instance and assign an async connection
pool to it:

``` python
from dinao.backend import create_connection_pool
from dinao.binding import AsyncFunctionBinder

binder = AsyncFunctionBinder()

pool = create_connection_pool(
    "postgresql+psycopg+async://myuser:mypass"
    "@localhost:5432/mydb"
)
binder.pool = pool
```

> **Note:** `AsyncFunctionBinder` validates that the pool is an
> `AsyncConnectionPool`. Passing a synchronous pool raises
> `AsyncPoolRequiredError`.

### Binding Functions

The `@binder.query`, `@binder.execute`, and `@binder.transaction`
decorators work the same way as their synchronous counterparts, but the
decorated functions must be `async`:

``` python
from dataclasses import dataclass

@dataclass
class User:
    id: int
    name: str

@binder.execute(
    "INSERT INTO users (name) VALUES (#{name})"
)
async def create_user(name: str) -> int:
    pass

@binder.query(
    "SELECT id, name FROM users WHERE id = #{user_id}"
)
async def get_user(user_id: int) -> User:
    pass

@binder.query(
    "SELECT id, name FROM users"
)
async def list_users() -> list[User]:
    pass

@binder.transaction()
async def create_and_get(name: str) -> User:
    await create_user(name=name)
    return await get_user(user_id=1)
```

Calling bound async functions requires `await`:

``` python
user = await get_user(user_id=42)
users = await list_users()
rows = await create_user(name="Alice")
```

### Async Generators

For streaming large result sets, use an `AsyncGenerator` return type
annotation:

``` python
from typing import AsyncGenerator

@binder.query(
    "SELECT id, name FROM users"
)
async def stream_users() -> AsyncGenerator[User, None]:
    pass

async for user in stream_users():
    print(user.name)
```

### Pool Lifecycle

Async connection pools should be disposed of when the application shuts
down. The `dispose` method on the pool is a coroutine:

``` python
await pool.dispose()
```
