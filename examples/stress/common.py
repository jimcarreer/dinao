"""Shared utilities for the stress test examples."""

import argparse
import dataclasses
import random
import threading
import uuid
from datetime import datetime, timezone

MAX_ERROR_SAMPLES = 5

_SQLITE_PK = "INTEGER PRIMARY KEY AUTOINCREMENT"
_POSTGRES_PK = "SERIAL PRIMARY KEY"

_DEFAULT_POSTGRES_SYNC_URL = "postgresql+psycopg://postgres:postgres@localhost:15432/dinao_stress"
_DEFAULT_POSTGRES_ASYNC_URL = "postgresql+psycopg+async://postgres:postgres@localhost:15432/dinao_stress"


@dataclasses.dataclass
class BackendConfig:
    """Backend-specific configuration for a stress test run."""

    name: str
    sync_url: str
    async_url: str
    pk_col_type: str
    row_lock: str


@dataclasses.dataclass
class Account:
    """An account row."""

    account_id: int
    name: str
    balance: int
    ref_id: str
    interest_rate: float
    risk_score: str
    created_at: str


@dataclasses.dataclass
class Transfer:
    """A ledger transfer row."""

    transfer_id: int
    from_account: int
    to_account: int
    amount: int
    ref_id: str
    created_at: str


@dataclasses.dataclass
class ErrorSample:
    """A single sampled error with type and message."""

    exc_type: str
    message: str


@dataclasses.dataclass
class ErrorCategory:
    """Aggregated count and samples for one error category."""

    count: int = 0
    samples: list[ErrorSample] = dataclasses.field(default_factory=list)


class ErrorTracker:
    """Thread-safe error tracker that categorizes exceptions.

    Errors are classified as expected or unexpected based on
    registered patterns.  Each category keeps a count and a limited
    number of sampled messages for diagnostics.
    """

    def __init__(self, max_samples: int = MAX_ERROR_SAMPLES, on_error=None):
        """Construct an error tracker.

        :param max_samples: maximum samples to retain per category
        :param on_error: optional callback(expected, category, message)
        """
        self._lock = threading.Lock()
        self._max_samples = max_samples
        self._matchers: list[tuple[type, str, str]] = []
        self.expected: dict[str, ErrorCategory] = {}
        self.unexpected: dict[str, ErrorCategory] = {}
        self._on_error = on_error

    def expect(self, exc_class: type, pattern: str, category: str) -> "ErrorTracker":
        """Register an expected error pattern.

        :param exc_class: exception class to match
        :param pattern: substring that must appear in the message
        :param category: label for this error category
        :returns: self, for chaining
        """
        self._matchers.append((exc_class, pattern, category))
        return self

    def record(self, exc: Exception) -> bool:
        """Record an exception, returning True if it was expected.

        :param exc: the caught exception
        :returns: whether the error matched a registered pattern
        """
        exc_type = type(exc).__name__
        exc_msg = str(exc)
        for cls, pattern, category in self._matchers:
            if isinstance(exc, cls) and pattern in exc_msg:
                with self._lock:
                    self._record_to(self.expected, category, exc_type, exc_msg)
                if self._on_error is not None:
                    self._on_error(True, category, exc_msg)
                return True
        with self._lock:
            self._record_to(self.unexpected, exc_type, exc_type, exc_msg)
        if self._on_error is not None:
            self._on_error(False, exc_type, exc_msg)
        return False

    def _record_to(self, bucket: dict, key: str, exc_type: str, exc_msg: str):
        """Add one error to the given bucket under the given key."""
        if key not in bucket:
            bucket[key] = ErrorCategory()
        entry = bucket[key]
        entry.count += 1
        if len(entry.samples) < self._max_samples:
            entry.samples.append(ErrorSample(exc_type, exc_msg))

    @property
    def expected_count(self) -> int:
        """Total number of expected errors across all categories."""
        return sum(c.count for c in self.expected.values())

    @property
    def unexpected_count(self) -> int:
        """Total number of unexpected errors across all categories."""
        return sum(c.count for c in self.unexpected.values())


def build_error_tracker(max_samples: int = MAX_ERROR_SAMPLES, on_error=None) -> ErrorTracker:
    """Create an ErrorTracker pre-loaded with common backend error patterns.

    Registers patterns for both SQLite and PostgreSQL contention
    errors using the base ``Exception`` class with substring matching,
    so no driver-specific imports are needed.

    :param max_samples: maximum samples to retain per category
    :param on_error: optional callback(expected, category, message)
    :returns: a configured ErrorTracker
    """
    tracker = ErrorTracker(max_samples=max_samples, on_error=on_error)
    tracker.expect(Exception, "database is locked", "database_locked")
    tracker.expect(Exception, "deadlock detected", "deadlock")
    tracker.expect(ValueError, "Insufficient funds", "insufficient_funds")
    tracker.expect(ValueError, "Both accounts must exist", "missing_account")
    return tracker


@dataclasses.dataclass
class StressResult:
    """Aggregate results from a stress test run."""

    elapsed: float
    total_ops: int
    final_accounts: int
    errors: ErrorTracker


def rand_name() -> str:
    """Return a short random account name."""
    return f"acct_{uuid.uuid4().hex}"


def rand_account_data() -> tuple:
    """Return a tuple of randomized account field values.

    :returns: (name, balance, ref_id, interest_rate, risk_score, created_at)
    """
    name = rand_name()
    balance = random.randint(100, 10_000)
    ref_id = str(uuid.uuid4())
    interest_rate = round(random.uniform(0.5, 15.0), 4)
    risk_score = str(complex(round(random.uniform(0, 10), 2), round(random.uniform(0, 10), 2)))
    created_at = datetime.now(timezone.utc).isoformat()
    return (name, balance, ref_id, interest_rate, risk_score, created_at)


def rand_transfer_ref() -> tuple:
    """Return a tuple of randomized transfer reference values.

    :returns: (ref_id, created_at)
    """
    return (str(uuid.uuid4()), datetime.now(timezone.utc).isoformat())


def parse_stress_args(description: str) -> argparse.Namespace:
    """Parse CLI arguments for stress tests.

    :param description: help text for the argument parser
    :returns: parsed namespace with seconds, workers, backend, and url
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--seconds", type=int, default=10, help="How many seconds to run (default 10)")
    parser.add_argument("--workers", type=int, default=3, help="Workers per role (default 3)")
    parser.add_argument(
        "--backend",
        choices=["sqlite", "postgres"],
        default="sqlite",
        help="Database backend (default sqlite)",
    )
    parser.add_argument("--url", type=str, default=None, help="Override connection URL")
    return parser.parse_args()


def _pool_url(base_url: str, workers: int) -> str:
    """Append pool sizing query params to a Postgres URL.

    :param base_url: base connection URL (no query string)
    :param workers: number of workers per role
    :returns: URL with pool_min_conn and pool_max_conn params
    """
    pool_max = min(workers * 5 + 5, 50)
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}pool_min_conn=5&pool_max_conn={pool_max}"


def build_backend_config(args: argparse.Namespace) -> BackendConfig:
    """Build a BackendConfig from parsed CLI arguments.

    :param args: parsed namespace from ``parse_stress_args``
    :returns: a BackendConfig with URLs and pk_col_type set
    """
    if args.backend == "postgres":
        sync_url = args.url if args.url else _pool_url(_DEFAULT_POSTGRES_SYNC_URL, args.workers)
        async_url = args.url if args.url else _pool_url(_DEFAULT_POSTGRES_ASYNC_URL, args.workers)
        return BackendConfig(
            name="PostgreSQL",
            sync_url=sync_url,
            async_url=async_url,
            pk_col_type=_POSTGRES_PK,
            row_lock="FOR UPDATE",
        )
    db_tag = uuid.uuid4().hex[:8]
    sync_path = f"/tmp/dinao_sync_stress_{db_tag}.db"
    async_path = f"/tmp/dinao_async_stress_{db_tag}.db"
    return BackendConfig(
        name="SQLite",
        sync_url=args.url if args.url else f"sqlite3://{sync_path}",
        async_url=args.url if args.url else f"sqlite3+aiosqlite://{async_path}",
        pk_col_type=_SQLITE_PK,
        row_lock="",
    )


def print_summary(result: StressResult):
    """Print a human-readable summary of stress test results.

    :param result: the results to summarize
    """
    print(
        f"\nFinished in {result.elapsed:.1f}s  "
        f"total_ops={result.total_ops}  "
        f"accounts_remaining={result.final_accounts}"
    )
    _print_error_section("Expected errors", result.errors.expected)
    _print_error_section("Unexpected errors", result.errors.unexpected)


def _print_error_section(heading: str, categories: dict[str, ErrorCategory]):
    """Print one section of the error report."""
    total = sum(c.count for c in categories.values())
    print(f"\n{heading} ({total} total):")
    if not categories:
        print("  (none)")
        return
    for name, cat in sorted(categories.items()):
        print(f"  {name}: {cat.count}")
        for sample in cat.samples:
            print(f"    [{sample.exc_type}] {sample.message}")
