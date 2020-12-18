DINAO Is Not An ORM
===================

What is DINAO? Well it might be easier to tell you what its not.  DINAO Is Not
An ORM.  It's not that I hate ORMs, I think in the right context they can be
quite handy.  I also think they're a bit "heavy weight" for some applications
and sometimes you need something a bit "lighter".  DINAO tries to be that tool:
smoothing out the more annoying or repetitive parts of database use in Python
while staying out of your way otherwise.

The APIs implemented mirror libraries I've used in other ecosystems.
Specifically you may notice similarities to JDBI's Declarative API or MyBatis's
interface mappers.  This is because I very much *like* this approach.  You're
the developer, I'm just here to reduce the number of lines of code you have to
write to meet your goal.  At the end of the day you know your schema and
database better than I do, and so you know what kinds of queries you need to
write better than I do.

**How do you pronounce DINAO?**

You pronounce it "Dino" like "Dinosaur".  Going back to plain old SQL probably
seems rather archaic after all.

Installation
------------

We're not yet in pypi as this is a very early Beta.  Installing from source is
recommended:

.. code-block::

    $ git clone git@github.com:jimcarreer/dinao.git
    $ cd dinao/
    $ pip install .

Basic Usage
***********

DINAO focuses binding functions to scoped connections / transactions against
the database and using function signatures and type hinting to infer mapping
and query parameterization.

.. code-block:: python

    from dinao.backend import create_connection_pool
    from dinao.binding import FunctionBinder

    con_url = "postgresql://test_user:test_pass@localhost:5432/test_db"
    db_pool = create_connection_pool(con_url)
    binder = FunctionBinder(db_pool)


    @binder.execute(
        "CREATE TABLE IF NOT EXISTS my_table (\n"
        "  name VARCHAR(32) PRIMARY KEY,\n"
        "  value INTEGER DEFAULT 0\n"
        ")"
    )
    def make_table():
        pass


    @binder.execute(
        "INSERT INTO my_table (name, value) VALUES(#{name}, #{value})\n"
        "ON CONFLICT (name) DO UPDATE\n"
        "  SET value = #{value}\n"
        "WHERE my_table.name = #{name}"
    )
    def upsert(name: str, value: int):
        pass


    @binder.query("SELECT name, value FROM my_table WHERE my_table.name LIKE #{search_term}")
    def search(search_term: str):
        pass


    @binder.transaction()
    def populate():
        make_table()
        upsert("testing", 52)
        upsert("test", 39)
        upsert("other_thing", 20)


    if __name__ == '__main__':
        populate()
        for row in search("test%"):
            n, v = row
            print(f"{n}: {v}")
