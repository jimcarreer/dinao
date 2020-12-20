Flask DINAO Example
===================

A simple example of how to use DINAO with flask.  To run this example you need:

* docker
* docker-compose

Additionally you will want to install ``requests`` to be able to run
``tester.py``.

You simply need to run:

.. code-block::

    $ docker-compose build
    $ docker-compose up
    $ python3 tester.py


Special Notes
*************

To prevent connections and underlying connection pools from being shared among
worker threads, it is recommended to add the ``defer=True`` argument to your
connection URL as an extra argument.  It is also recommended, when using DINAO
with Flask (and other web frameworks) in production, that your connection pool
minimum and maximum connections are both set to 1 and the use of WSGI workers
used for scaling.
