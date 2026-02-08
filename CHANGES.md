# Change Log

## <NEXT VERSION>

Place holder for next version.

### Breaking Changes

### Features

### Chores

### Bug Fixes

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
