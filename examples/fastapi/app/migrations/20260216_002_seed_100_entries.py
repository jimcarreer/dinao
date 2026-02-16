"""Pre-populate the data table with 100 entries."""


async def upgrade(cnx):
    """Insert 100 rows into the data table."""
    for i in range(1, 101):
        await cnx.execute(
            "INSERT INTO data (name, value) VALUES (#{name}, #{value})",
            name=f"entry_{i:03d}",
            value=i,
        )
