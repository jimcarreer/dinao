"""Sample migration script that creates a users table."""


def upgrade(cnx):
    """Create the users table."""
    cnx.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
