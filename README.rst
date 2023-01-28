DINAO Is Not An ORM
===================
|code-of-conduct| |build-status| |cover-status| |pyver-status| |pypiv-status| |coding-style|

Introduction
------------

What is DINAO? Well it might be easier to tell you what it's not.  DINAO Is Not
An ORM.  If you want an ORM, `SQLAlchemy`_ is absolutely the best python has to
offer.

Target Audience
***************

Do you like writing SQL? Do you hate all the boiler plate involved with setting
up connections and cursors then cleaning them up?  Would you just like
something simple that executes a query and can map the results to simple data
classes?  Then DINAO is for you!

Influences and Guiding Principles
*********************************

The APIs implemented mirror libraries I've used in other ecosystems.
Specifically, you may notice similarities to the JDBI Declarative API or the
MyBatis interface mappers.  This is because I very much *like* this approach.
You're the developer, I'm just here to reduce the number of lines of code you
have to write to meet your goal.  At the end of the day you know your schema
and database better than I do, and so you know what kinds of queries you need
to write better than I do.

How do you pronounce DINAO?
***************************

You pronounce it "Dino" like "Dinosaur".  Going back to plain old SQL probably
seems rather archaic after all.

Usage
-----

Install via pip:

.. code-block::

    $ pip install dinao

You will also need to install your backend driver.  Backends + drivers
supported are:

* Sqlite3 via Python's standard library
* PostgreSQL via psycopg2

Basic Example
*************

DINAO focuses binding functions to scoped connections / transactions against
the database and using function signatures and type hinting to infer mapping
and query parameterization.

Below shows a simple example of DINAO usage. For more comprehensive usage and
feature showcase see `examples`_.

.. code-block:: python

    from typing import List
    from dataclasses import dataclass
    from dinao.backend import create_connection_pool, Connection
    from dinao.binding import FunctionBinder

    binder = FunctionBinder()

    @dataclass
    class MyModel:
        name: str
        value: int

    @binder.execute(
        "CREATE TABLE IF NOT EXISTS my_table ( "
        "  name VARCHAR(32) PRIMARY KEY, "
        "  value INTEGER DEFAULT 0"
        ")"
    )
    def make_table():
        pass


    @binder.execute(
        "INSERT INTO my_table (name, value) VALUES(#{model.name}, #{model.value}) "
        "ON CONFLICT (name) DO UPDATE "
        "  SET value = #{model.value} "
        "WHERE my_table.name = #{model.name}"
    )
    def upsert(model: MyModel) -> int:
        pass


    # This is an example of a query where a template variable is directly
    # replaced in a template.  This is via a template argument denoted with
    # !{column_name}.  The #{search_term} on the other hand uses proper
    # escaping and parameterization in the underlying SQL engine.
    #
    # IMPORTANT: This is a vector for SQL Injection, do not use direct template
    #            replacement on untrusted inputs, especially those coming from
    #            users.  Ensure that you validate, restrict, or otherwise limit
    #            the values that can be used in direct template replacement.
    #
    @binder.query("SELECT name, value FROM my_table WHERE !{column_name} LIKE #{search_term}")
    def search(column_name: str, search_term: str) -> List[MyModel]:
        pass


    @binder.transaction()
    def populate(cnx: Connection = None):
        make_table()
        cnx.commit()
        upsert(MyModel("testing", 52))
        upsert(MyModel("test", 39))
        upsert(MyModel("other_thing", 20))


    if __name__ == '__main__':
        con_url = "sqlite3:///tmp/example.db"
        db_pool = create_connection_pool(con_url)
        binder.pool = db_pool
        populate()
        for model in search("name", "test%"):
            print(f"{model.name}: {model.value}")

Contributing
------------

Check out our `code of conduct`_ and `contributing documentation`_.

Release Process
---------------

This library adheres too `semantic versioning 2.0.0`_ standards, in general
that means, given a version number MAJOR.MINOR.PATCH, increment:

 1. MAJOR version when you make incompatible API changes
 2. MINOR version when you add functionality in a backwards compatible manner
 3. PATCH version when you make backwards compatible bug fixes

Changes for the next version should be accumulated on the main branch until
such time that there is enough confidence in the build that it can be released.
When this is done, a repository administrator opens a PR to bump the version in
`__version__.py` updates the change logs, merges this PR then tags the merge
with the release version.  Only tagged commits of main are built and published.


.. |build-status| image:: https://github.com/jimcarreer/dinao/workflows/Build/badge.svg?branch=main
   :target: https://github.com/jimcarreer/dinao
.. |cover-status| image:: https://codecov.io/gh/jimcarreer/dinao/branch/main/graph/badge.svg?token=CpJ5u1ngZH
   :target: https://codecov.io/gh/jimcarreer/dinao
.. |pyver-status| image:: https://img.shields.io/pypi/pyversions/dinao
   :target: https://pypi.org/project/dinao/
.. |pypiv-status| image:: https://badge.fury.io/py/dinao.svg?dummy
   :target: https://pypi.org/project/dinao/
.. |coding-style| image:: https://img.shields.io/badge/code%20style-black-000000.svg
   :target: https://github.com/psf/black
.. |code-of-conduct| image:: https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg
   :target: CODE_OF_CONDUCT.rst

.. _SQLAlchemy: https://sqlalchemy.org/
.. _examples: https://github.com/jimcarreer/dinao/tree/main/examples
.. _code of conduct: https://github.com/jimcarreer/dinao/blob/main/CODE_OF_CONDUCT.rst
.. _contributing documentation: https://github.com/jimcarreer/dinao/blob/main/CONTRIBUTING.rst
.. _semantic versioning 2.0.0: https://semver.org/spec/v2.0.0.html
