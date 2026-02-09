"""Synchronous database interface for the stress example.

Defines bound functions against a multi-table schema and exercises
every major feature of the sync FunctionBinder: execute, query,
transaction, Optional returns, list returns, Generator streaming,
dict returns, bool returns, and direct connection access.
"""

from datetime import datetime
from typing import Generator, Optional
from uuid import UUID

from common import Account, Transfer, rand_transfer_ref

from dinao.binding import FunctionBinder

binder = FunctionBinder()
ROW_LOCK = ""


# -- DDL ----------------------------------------------------------------


@binder.execute("DROP TABLE IF EXISTS transfers")
def drop_transfers_table():
    """Drop the transfers table if it exists."""
    pass


@binder.execute("DROP TABLE IF EXISTS accounts")
def drop_accounts_table():
    """Drop the accounts table if it exists."""
    pass


@binder.execute(
    "CREATE TABLE IF NOT EXISTS accounts ("
    "  id !{pk_col_type},"
    "  name TEXT UNIQUE NOT NULL,"
    "  balance INTEGER NOT NULL DEFAULT 0,"
    "  ref_id TEXT NOT NULL,"
    "  interest_rate REAL NOT NULL DEFAULT 0.0,"
    "  risk_score TEXT NOT NULL DEFAULT '(0+0j)',"
    "  created_at TEXT NOT NULL"
    ")"
)
def create_accounts_table(pk_col_type: str):
    """Create the accounts table."""
    pass


@binder.execute(
    "CREATE TABLE IF NOT EXISTS transfers ("
    "  id !{pk_col_type},"
    "  from_account INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,"
    "  to_account   INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,"
    "  amount       INTEGER NOT NULL,"
    "  ref_id       TEXT NOT NULL,"
    "  created_at   TEXT NOT NULL"
    ")"
)
def create_transfers_table(pk_col_type: str):
    """Create the transfers table."""
    pass


@binder.transaction()
def init_schema(pk_col_type: str):
    """Drop and recreate all tables inside a single transaction."""
    drop_transfers_table()
    drop_accounts_table()
    create_accounts_table(pk_col_type)
    create_transfers_table(pk_col_type)


# -- Accounts -----------------------------------------------------------


@binder.execute(
    "INSERT INTO accounts (name, balance, ref_id, interest_rate, risk_score, created_at) "
    "VALUES (#{name}, #{balance}, #{ref_id}, #{interest_rate}, #{risk_score}, #{created_at})"
)
def insert_account(name: str, balance: int, ref_id: str, interest_rate: float, risk_score: str, created_at: str) -> int:
    """Insert an account, return rows affected."""
    pass


@binder.query(
    "SELECT id AS account_id, name, balance, ref_id, interest_rate, risk_score, created_at "
    "FROM accounts WHERE id = #{account_id}"
)
def get_account(account_id: int) -> Optional[Account]:
    """Get a single account by id, or None."""
    pass


@binder.query(
    "SELECT id AS account_id, name, balance, ref_id, interest_rate, risk_score, created_at "
    "FROM accounts WHERE name = #{name} !{row_lock}"
)
def get_account_by_name(name: str, row_lock: str = "") -> Optional[Account]:
    """Get a single account by name, or None."""
    pass


@binder.query(
    "SELECT id AS account_id, name, balance, ref_id, interest_rate, risk_score, created_at "
    "FROM accounts ORDER BY name LIMIT #{page.limit} OFFSET #{page.offset}"
)
def list_accounts(page: dict) -> list[Account]:
    """Return a page of accounts."""
    pass


@binder.query(
    "SELECT id AS account_id, name, balance, ref_id, interest_rate, risk_score, created_at "
    "FROM accounts ORDER BY name"
)
def stream_accounts() -> Generator[Account, None, None]:
    """Stream all accounts one at a time."""
    pass


@binder.query("SELECT EXISTS(SELECT 1 FROM accounts WHERE name = #{name})")
def account_exists(name: str) -> bool:
    """Check whether an account with the given name exists."""
    pass


@binder.query("SELECT COUNT(*) as cnt FROM accounts")
def count_accounts() -> int:
    """Return the total number of accounts."""
    pass


@binder.execute("UPDATE accounts SET balance = balance + #{delta} WHERE id = #{account_id}")
def adjust_balance(account_id: int, delta: int) -> int:
    """Adjust an account balance by delta."""
    pass


@binder.execute("DELETE FROM accounts WHERE id = #{account_id}")
def delete_account(account_id: int) -> int:
    """Delete an account."""
    pass


# -- Single-value queries (one per NATIVE_SINGLE type) ------------------


@binder.query("SELECT name FROM accounts WHERE id = #{account_id}")
def get_account_name(account_id: int) -> Optional[str]:
    """Look up an account name by id."""
    pass


@binder.query("SELECT interest_rate FROM accounts WHERE id = #{account_id}")
def get_interest_rate(account_id: int) -> Optional[float]:
    """Look up an account interest rate by id."""
    pass


@binder.query("SELECT ref_id FROM accounts WHERE id = #{account_id}")
def get_account_ref(account_id: int) -> Optional[UUID]:
    """Look up an account reference id by id."""
    pass


@binder.query("SELECT created_at FROM accounts WHERE id = #{account_id}")
def get_account_created(account_id: int) -> Optional[datetime]:
    """Look up an account creation timestamp by id."""
    pass


@binder.query("SELECT risk_score FROM accounts WHERE id = #{account_id}")
def get_risk_score(account_id: int) -> Optional[complex]:
    """Look up an account risk score by id."""
    pass


# -- Transfers ----------------------------------------------------------


@binder.execute(
    "INSERT INTO transfers (from_account, to_account, amount, ref_id, created_at) "
    "VALUES (#{from_id}, #{to_id}, #{amount}, #{ref_id}, #{created_at})"
)
def insert_transfer(from_id: int, to_id: int, amount: int, ref_id: str, created_at: str) -> int:
    """Record a transfer row."""
    pass


@binder.query(
    "SELECT id AS transfer_id, from_account, to_account, amount, ref_id, created_at "
    "FROM transfers WHERE from_account = #{account_id} OR to_account = #{account_id} "
    "ORDER BY id"
)
def transfers_for(account_id: int) -> list[Transfer]:
    """List all transfers involving a given account."""
    pass


@binder.query(
    "SELECT id AS transfer_id, from_account, to_account, amount, ref_id, created_at "
    "FROM transfers WHERE from_account = #{account_id} OR to_account = #{account_id} "
    "ORDER BY id"
)
def stream_transfers_for(account_id: int) -> Generator[Transfer, None, None]:
    """Stream transfers for a given account."""
    pass


@binder.query("SELECT COALESCE(SUM(amount), 0) FROM transfers WHERE from_account = #{account_id}")
def total_sent(account_id: int) -> int:
    """Total amount sent from a given account."""
    pass


@binder.query("SELECT COALESCE(SUM(amount), 0) FROM transfers WHERE to_account = #{account_id}")
def total_received(account_id: int) -> int:
    """Total amount received by a given account."""
    pass


# -- Transactional operations -------------------------------------------


@binder.transaction()
def transfer(from_name: str, to_name: str, amount: int) -> dict:
    """Move funds between two accounts atomically.

    Returns a dict with the resulting balances.
    """
    src = get_account_by_name(from_name, row_lock=ROW_LOCK)
    dst = get_account_by_name(to_name, row_lock=ROW_LOCK)
    if src is None or dst is None:
        raise ValueError("Both accounts must exist")
    if src.balance < amount:
        raise ValueError(f"Insufficient funds: {src.name} has {src.balance}, need {amount}")
    adjust_balance(src.account_id, -amount)
    adjust_balance(dst.account_id, amount)
    ref_id, created_at = rand_transfer_ref()
    insert_transfer(src.account_id, dst.account_id, amount, ref_id, created_at)
    return {
        "from": get_account(src.account_id),
        "to": get_account(dst.account_id),
    }


@binder.transaction()
def bulk_create_accounts(account_data: list) -> int:
    """Create many accounts in one transaction, return count created."""
    created = 0
    for name, balance, ref_id, interest_rate, risk_score, created_at in account_data:
        created += insert_account(name, balance, ref_id, interest_rate, risk_score, created_at)
    return created
