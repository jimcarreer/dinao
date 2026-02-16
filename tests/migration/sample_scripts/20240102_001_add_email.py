"""Sample migration script that adds an email column."""


def upgrade(cnx):
    """Add email column to users table."""
    cnx.execute("ALTER TABLE users ADD COLUMN email TEXT")
