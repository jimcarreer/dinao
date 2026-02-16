"""Delete the last three seeded entries."""


async def upgrade(cnx):
    """Remove entry_098, entry_099, and entry_100 from the data table."""
    for i in range(98, 101):
        await cnx.execute(
            "DELETE FROM data WHERE name = #{name}",
            name=f"entry_{i:03d}",
        )
