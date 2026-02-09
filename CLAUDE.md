# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DINAO (DINAO Is Not An ORM, recursive acronym) is a Python library for simple database access without a full ORM.
Developers write SQL directly; DINAO provides function binding via decorators, automatic result mapping to Python types,
and connection pooling. Supports multiple relational database backends.

## Preflight Checks for New Feature Work

Before starting any new feature or bug fix, perform these steps:

1. Ensure you are on the `main` branch (`git branch --show-current`)
2. Ensure `main` is up-to-date with the remote (`git fetch origin && git
   diff main origin/main`)
3. Create (or checkout) a feature branch for the new work

## Common Commands

### Enabling Local Environment

**Important**: When running tests or quality jobs, always enable the local python environment for this project first.

```bash
source ./.venv/bin/activate
```

### Testing

```bash
# Run all tests (requires docker containers for integration tests)
pytest

# Run a single test file
pytest tests/binding/test_binders.py

# Run a single test
pytest tests/binding/test_binders.py::test_function_name

# Run only unit tests (no database containers needed)
pytest tests/binding/

# Start integration test databases
docker compose -f tests/docker-compose.yaml up -d
```

### Linting & Formatting
```bash
# Run all checks
flake8 ./dinao/ ./tests/
black --check --diff ./dinao/ ./tests/
pyspelling -v

# Auto-format with black
black ./dinao/ ./tests/
```

### Install for Development
```bash
pip install -e ".[dev]"
```

## Code Style

- **Line length**: 120 characters for code, 80 for standalone documentation (.rst files)
  - This includes documentation files, and things like mark down (including the CLAUDE.md) 
- **Formatter**: black
- **Docstrings**: Required on all public methods (enforced by flake8-docstrings), ReStructuredText format
- **Spelling**: Code and docstrings are spell-checked with aspell/pyspelling. Custom word list in `.pyspelling.xdic`
- **Coverage**: 99%+ required. Use `# pragma: no cover` only for unavoidable no-op sections
- **Warnings**: pytest treats all warnings as errors (`filterwarnings = ["error"]`)
- **Other Coding Preferences**:
  - Class based / Object oriented programming preferred
  - Common functionality between features should (as much as possible) be refactored up the class hierarchy, with 
    implementation specific functionality staying in child classes.
  - Never import into the body of a function, class or anywhere besides the top of a file
    - There is exactly one exception to this rule at this time: when importing the driver module for a given database
      backend
  - Prefer early returns to denest code; guard clauses at the top of a
    function or block are cleaner than deeply nested if/else trees.
  - Do not use `TYPE_CHECKING` to work around circular imports;
    instead refactor the code to eliminate the circular dependency.
  - Large functions and loop bodies should generally be broken up into smaller functions where applicable to help with
    context window sizes on specific tasks, and to help with readability.
  - When a new engine / database / backend is added, the backend
    documentation should be updated accordingly.
  - Avoid implicit string concatenation (adjacent string literals)
    when a single string will fit within the line length limit. Use
    one string instead of `"foo " "bar"`.

## Architecture

### Two-module design

**`dinao/backend/`** — Database connection abstraction layer
- `base.py`: Abstract classes `Connection`, `ConnectionPool`, `ResultSet` wrapping DB-API 2.0
- `sqlite.py`, `postgres.py`, `mariadb.py`, `mysql.py`: Per-database implementations
  - if a new "driver" library (e.g. `psycopg3`) is added for backend support, then the module for the database type
    should be converted to a multifile module, example:
    ```
    # Original file
    backend/postgres.py
    # New structure
    backend/postgres/
    backend/postgres/__init__.py
    backend/postgres/psycopg2.py
    backend/postgres/psycopg3.py
    ```
- `create_connection_pool(url)` factory in `__init__.py` selects backend from URL scheme 
  (e.g. `postgresql+psycopg2://...`)

**`dinao/binding/`** — Function-to-SQL binding via decorators
- `binders.py`: `FunctionBinder` is the main public API. Decorators `@binder.query(sql)` and `@binder.execute(sql)` 
   bind Python functions to SQL. `@binder.transaction()` scopes multiple calls to one connection. Uses
  `threading.local()` for thread-safe connection storage.
- `templating.py`: pyparsing-based SQL template grammar. `#{var}` = parameterized (safe), `!{var}` = direct
   substitution (injection risk). Supports dotted property access like `#{user.name}`.
- `mappers.py`: Maps query results to Python types based on return type annotations. Supports dataclasses, Pydantic
   models, dicts, native types (str, int, etc.), and generators for streaming large result sets.

### How binding works

A decorated function's signature provides SQL template parameters, and its return type annotation determines result
mapping:
1. Template parameters are extracted from function args at call time
2. SQL is rendered via the template engine (`#{}`/`!{}` substitution)
3. Query is executed on a connection from the pool (thread-local)
4. Results are mapped to the return type via the appropriate `RowMapper`

### Integration tests

Integration tests (`tests/backend/`) require Docker containers defined in `tests/docker-compose.yaml`:
- PostgreSQL on port 15432
- MySQL on port 23306
- MariaDB on port 13306

All containers use TLS certs from `tests/vols/tls/`, this is mainly for the purposes of testing connection pooling
function where TLS is used.
