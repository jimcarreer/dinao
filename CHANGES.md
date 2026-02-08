# Change Log

## NEXT VERSION PLACEHOLDER

-   Placeholder for next version.

## 2.1.0

Async support, psycopg (v3) backend, and project modernization.

### Breaking Changes

-   Internal class renames: `Bounded*` classes renamed to `Bound*`
    (`BoundedFunction` -> `BoundFunction`, `BoundedQuery` ->
    `BoundQuery`, etc.). These were not part of the public API
    exports but may affect code importing them directly.
-   Internal module restructuring: `dinao/binding/binders.py` and
    `dinao/backend/postgres.py` converted from single files to
    packages. Public import paths through `dinao.binding` and
    `dinao.backend` are preserved.

### Features

-   Full async support for binding and backend layers, including
    `AsyncFunctionBinder`, `AsyncConnectionPool`, and
    `AsyncConnection`
-   psycopg (v3) backend support for PostgreSQL, both sync and
    async modes (`postgresql+psycopg://` and
    `postgresql+psycopg+async://`)
-   FastAPI example demonstrating async usage
-   Backend documentation (`BACKENDS.md`) covering all supported
    databases, URL formats, and async usage
-   CI preview publish job for dev builds to PyPI

### Chores

-   Migrate documentation from reStructuredText to Markdown
-   Remove tox in favor of direct tool invocation in CI
-   Move docker-compose and test volumes into `tests/` directory
-   Add `CLAUDE.md` for Claude Code guidance
-   Refactor test fixtures into shared conftest modules
-   Add comprehensive async binder test suite
-   Update pyspelling and spelling dictionary
-   Fix codecov token handling in CI

### Bug Fixes

-   MySQL backend error message incorrectly referenced mariadb
    module instead of mysql-connector-python

## 2.0.0

General refresh and minor bug fixes

### Breaking Changes

-   Drop support for python 3 before 3.10

### Chores

-   Add testing and support for python 3.12+
-   Add example for pydantic
-   Modernize and refresh build

### Bug Fixes

-   Missing mapping for native bool
-   Optional / Nullable Union type should be allowed

## 1.4.0

Support for MySQL

### Features

-   MySQL DB Support

## 1.3.0

TLS support for current backends

### Features

-   Basic TLS support for Maria DB connection pools
-   Basic TLS support for Postgres connection pools

## 1.2.0

Small fixes, mariadb support via maria db connector

### Features

-   Maria DB support

### Bug Fixes

-   Assertion no longer used in 'production code'
-   Empty query string arguments in database connection strings now
    retained
-   Postgres minimum and maximum connections for pool no longer allowed
    to be negative

## 1.1.1

Minor documentation updates, add coverage / testing on 3.11.

## 1.1.0

Release for direct template replacement.

### Features

-   Direct template replacement is now possible.

## 1.0.2

Release for very annoying bug.

### Bug Fixes

-   SQLite backend now supports relative paths and user home expansion

## 1.0.1

No code changes, trivial release to test migration from travis to github
actions.

## 1.0.0

Initial release
