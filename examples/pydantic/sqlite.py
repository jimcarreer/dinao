"""Example showing dinao with Pydantic models using SQLite."""

from dinao.backend import create_connection_pool

import dbi

if __name__ == "__main__":
    con_url = "sqlite3:///tmp/pydantic_example.db"
    db_pool = create_connection_pool(con_url)
    dbi.binder.pool = db_pool

    # Populate sample data
    dbi.populate()

    # Query results are returned as Pydantic models
    print("Search results (Pydantic models):")
    for entry in dbi.search("test%", {"limit": 10, "offset": 0}):
        # entry is a DataEntry Pydantic model
        print(f"  {entry.name}: {entry.value}")
        # Pydantic models serialize to JSON easily
        print(f"    JSON: {entry.model_dump_json()}")

    # Single result queries also work
    print("\nSingle entry lookup:")
    entry = dbi.get_by_name("testing")
    if entry:
        print(f"  Found: {entry}")
        print(f"  As dict: {entry.model_dump()}")

    # Aggregations map to Pydantic models too
    print("\nStats (Pydantic model from aggregation):")
    stats = dbi.get_stats()
    print(f"  Total entries: {stats.total_entries}")
    print(f"  Total value: {stats.total_value}")
    print(f"  JSON: {stats.model_dump_json()}")
