"""Integration test for bool casting with SQLite backend."""

from dinao.backend import create_connection_pool
from dinao.binding.binders import FunctionBinder


def test_sqlite_bool_return(tmp_sqlite3_db_url: str):
    """Tests that a bool return type is correctly cast from SQLite integer."""
    pool = create_connection_pool(tmp_sqlite3_db_url)
    binder = FunctionBinder()
    binder.pool = pool

    @binder.execute("CREATE TABLE users (name TEXT PRIMARY KEY)")
    def create_table() -> None:
        pass  # pragma: no cover

    @binder.execute("INSERT INTO users (name) VALUES (#{name})")
    def insert_user(name: str) -> int:
        pass  # pragma: no cover

    @binder.query("SELECT EXISTS(SELECT 1 FROM users WHERE name = #{name})")
    def user_exists(name: str) -> bool:
        pass  # pragma: no cover

    create_table()
    insert_user("alice")

    assert user_exists("alice") is True
    assert user_exists("bob") is False

    pool.dispose()
