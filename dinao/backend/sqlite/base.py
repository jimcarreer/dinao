"""Shared base classes for SQLite backends."""

import os.path


class ConnectionPoolSQLiteMixin:
    """Mixin providing shared SQLite URL parsing and connection kwargs construction."""

    def _url_to_cnx_kwargs(self):
        """Parse the URL and construct connection keyword arguments.

        :returns: a dictionary of connection keyword arguments
        """
        file_path = os.path.abspath(os.path.expanduser(self._db_url.path))
        return {"database": file_path}
