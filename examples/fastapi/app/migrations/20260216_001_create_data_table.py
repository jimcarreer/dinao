"""Create the data table used by the FastAPI example."""


async def upgrade(cnx):
    """Create the data table with name and value columns."""
    await cnx.execute(
        "CREATE TABLE IF NOT EXISTS data ("
        "  name VARCHAR(256) PRIMARY KEY,"
        "  value INTEGER DEFAULT 0"
        ")"
    )
