Flask DINAO Example
===================

A simple example of how to use DINAO with flask.  To run this example you need:

* docker
* docker-compose

Additionally you will want to install the python library ``requests`` to be
able to run ``tester.py``.

You simply need to run:

.. code-block::

    $ ./build.sh
    $ docker-compose up
    $ pip install requests
    $ python3 tester.py


Special Notes
*************

For applications making use of WSGI, a pool of connections is generally not
required, as the pool should not be shared among workers.  Instead set your
minimum / maximum pool size to 1.

It is important not to initialize the database pool used by the binder before
the process forks.  This is why the `example`_ initializes the pool and sets
it on the binder in ``before_first_request``.  Reusing pools, even thread safe
ones, across WSGI workers will result in errors.

See this `explanation`_ for more information on this subject.

.. _gunicorn: https://docs.gunicorn.org/en/stable/settings.html#preload-app
.. _example: https://github.com/jimcarreer/dinao/blob/main/examples/flask/app/api.py#L20
.. _explanation: https://davidcaron.dev/sqlalchemy-multiple-threads-and-processes/
