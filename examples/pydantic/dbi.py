"""Database interface using Pydantic models for result mapping.

This example demonstrates that dinao works out of the box with Pydantic models.
The ClassRowMapper passes query results as keyword arguments to the model
constructor, which is exactly how Pydantic models are instantiated.
"""

from typing import Optional

from dinao.binding import FunctionBinder
from pydantic import BaseModel, Field

binder = FunctionBinder()


class DataEntry(BaseModel):
    """Pydantic model for data entries with automatic validation."""

    name: str = Field(max_length=256)
    value: int = Field(ge=0)


class DataStats(BaseModel):
    """Pydantic model for aggregated statistics."""

    total_entries: int
    total_value: int


@binder.execute(
    "CREATE TABLE IF NOT EXISTS data ( " "  name VARCHAR(256) PRIMARY KEY, " "  value INTEGER DEFAULT 0" ")"
)
def make_table():
    """Create the data table if it does not exist."""
    pass


@binder.execute(
    "INSERT INTO data (name, value) VALUES(#{name}, #{value}) "
    "ON CONFLICT (name) DO UPDATE "
    "  SET value = #{value} "
    "WHERE data.name = #{name}"
)
def upsert(name: str, value: int):
    """Insert or update a data entry."""
    pass


@binder.query("SELECT name, value FROM data WHERE data.name = #{name}")
def get_by_name(name: str) -> Optional[DataEntry]:
    """Get a single entry by name, returns None if not found."""
    pass


@binder.query(
    "SELECT name, value FROM data WHERE data.name LIKE #{search_term} "
    "ORDER BY data.name LIMIT #{page.limit} OFFSET #{page.offset}"
)
def search(search_term: str, page: dict) -> list[DataEntry]:
    """Search entries and return as Pydantic models."""
    pass


@binder.query("SELECT COUNT(*) as total_entries, COALESCE(SUM(value), 0) as total_value FROM data")
def get_stats() -> DataStats:
    """Get aggregate statistics as a Pydantic model."""
    pass


@binder.transaction()
def populate():
    """Set up the table and insert sample data."""
    make_table()
    upsert("testing", 52)
    upsert("test", 39)
    upsert("other_thing", 20)
    upsert("test", 50)  # Updates existing "test" entry
