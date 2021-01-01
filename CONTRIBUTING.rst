Contributing Guidelines
=======================

Make sure to read the `code of conduct`_ for guidelines on community
engagement.

Communication
-------------

There is no mailing list, dedicated chat server / channel.  Instead, unless
this library actually becomes popular, the #database channel of the `python
discord`_ server should be used.  Additionally, as much as possible should be
coordinated via github issues (questions, feature requests, etc ...).

Code Style and Linting
----------------------

The code styling is mostly `black styling`_ though exceptions are made when it
makes sense (see the template grammar definition in binding module).  The line
length of **120 characters** generally without exception.

In addition to black styling, the code is linted for code smells, poor style,
documentation, and other various "lint" issues via `flake8`_.

Documentation
-------------

All documentation should be written in `ReStructuredText`_.  Standalone README
and other miscellaneous documentation (CONTRIBUTING, CODE_OF_CONDUCT, etc ...)
should have a line length of **80 characters**.  For documentation strings in
code, the code line character limit (120 characters) is acceptable.

Spelling
********

Code, documentation, and documentation strings are spell checked with aspell
(with the help of the excellent `pyspelling`_ library) as part of the build
process.  A custom word list, applied on top of the standard American English
dictionary, can be found in the file .pyspelling.xdic.  This is just a plain
alphabetical list, new exceptions or missing words can be added as needed.

Testing
-------

Code coverage, as of the writing of this document, is at 99.9%.  In general any
PR, even for critical bug fixes, should also contain tests that cover the
changes in the PR.  In situations where ``pass`` or similar "no-op" code sections
are used, the ``# pragma: no cover`` can be used to remove the line for
consideration in the coverage report.

.. _black styling: https://github.com/psf/black
.. _python discord: https://discord.gg/python
.. _flake8: https://pypi.org/project/flake8/
.. _pyspelling: https://pypi.org/project/pyspelling/
.. _code of conduct: CODE_OF_CONDUCT.rst
.. _restructuredtext: https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html
