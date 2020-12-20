Flask DINAO Example
===================

A simple example of how to use DINAO with flask.  To run this example you need:

* docker
* docker-compose

Additionally you will want to install the python library ``requests`` to be
able to run ``tester.py``.

You simply need to run:

.. code-block::

    $ docker-compose build
    $ docker-compose up
    $ pip install requests
    $ python3 tester.py


Special Notes
*************

For applications making use of WSGI, a pool of connections is generally not
required, as the pool should not be shared among workers.  Instead set your
minimum / maximum pool size to 1.

When using ``--preload`` with `gunicorn`_, or similar facilities in other WSGI
servers, it is important to use the ``defer=True`` (`example`_) connection pool
argument in order to keep the same connections and underlying pool from being
reused among worker processes.

See this `explanation`_ for more information on this subject.

.. _gunicorn: https://docs.gunicorn.org/en/stable/settings.html#preload-app
.. _example: https://github.com/jimcarreer/dinao/blob/main/examples/flask/app/dbi.py#L4
.. _explanation: https://davidcaron.dev/sqlalchemy-multiple-threads-and-processes/
