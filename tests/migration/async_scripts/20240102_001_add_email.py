"""Sample async migration script that adds an email column."""


async def upgrade(cnx):
    """Add email column to users table."""
    await cnx.execute("ALTER TABLE users ADD COLUMN email TEXT")
