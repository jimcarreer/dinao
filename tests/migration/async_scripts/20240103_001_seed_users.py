"""Sample async migration script that pre-seeds users into the table."""


async def upgrade(cnx):
    """Insert seed data into the users table."""
    await cnx.execute(
        "INSERT INTO users (id, name, email) VALUES (#{id}, #{name}, #{email})",
        id=1,
        name="alice",
        email="alice@example.com",
    )
    await cnx.execute(
        "INSERT INTO users (id, name, email) VALUES (#{id}, #{name}, #{email})",
        id=2,
        name="bob",
        email="bob@example.com",
    )
    rows = await cnx.query("SELECT id, name, email FROM users ORDER BY id")
    assert len(rows) == 2
    assert rows[0]["name"] == "alice"
    assert rows[1]["name"] == "bob"
