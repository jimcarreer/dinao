DINAO Is Not An ORM
===================
|build-status| |cover-status| |pyver-status| |pypiv-status| |coding-style|

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

Install via pip:

.. code-block::

    $ pip install dinao

You will also need to install your backend driver.  Backends + drivers supported are:

* Sqlite3 (from the standard lib)
* PostgreSQL via psycopg2

Basic Usage
***********

DINAO focuses binding functions to scoped connections / transactions against
the database and using function signatures and type hinting to infer mapping
and query parameterization.

.. code-block:: python

    # pip install dinao
    # pip install psycopg2-binary

    from dinao.backend import create_connection_pool
    from dinao.binding import FunctionBinder

    con_url = "postgresql://test_user:test_pass@localhost:5432/test_db"
    db_pool = create_connection_pool(con_url)
    binder = FunctionBinder(db_pool)


    @binder.execute(
        "CREATE TABLE IF NOT EXISTS my_table ( "
        "  name VARCHAR(32) PRIMARY KEY, "
        "  value INTEGER DEFAULT 0"
        ")"
    )
    def make_table():
        pass


    @binder.execute(
        "INSERT INTO my_table (name, value) VALUES(#{name}, #{value}) "
        "ON CONFLICT (name) DO UPDATE "
        "  SET value = #{value} "
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

.. |build-status| image:: https://api.travis-ci.org/jimcarreer/dinao.svg?branch=main
   :target: https://travis-ci.org/jimcarreer/dinao
.. |cover-status| image:: https://codecov.io/gh/jimcarreer/dinao/dinao/branch/main/graph/badge.svg
   :target: https://codecov.io/gh/jimcarreer/dinao
.. |pyver-status| image:: https://img.shields.io/pypi/pyversions/dinao
   :target: https://pypi.org/project/dinao/
.. |pypiv-status| image:: https://badge.fury.io/py/dinao.svg?dummy
   :target: https://pypi.org/project/dinao/
.. |coding-style| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
