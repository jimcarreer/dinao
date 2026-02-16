"""Sample async migration script that creates a users table."""


async def upgrade(cnx):
    """Create the users table."""
    await cnx.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
