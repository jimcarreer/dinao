"""Migration script discovery and validation."""

import os
import re
from dataclasses import dataclass
from typing import List, Optional

from dinao.migration.errors import DiscoveryError

DEFAULT_PATTERN = r"^\d{8}_\d{3}_.+\.py$"


@dataclass
class MigrationScript:
    """Represents a discovered migration script file.

    :param name: the script filename without the ``.py`` extension
    :param path: the absolute path to the script file
    """

    name: str
    path: str


class ScriptDiscovery:
    """Scans a directory for migration scripts matching a naming pattern.

    Scripts are sorted lexicographically by filename and checked for
    duplicate names.
    """

    def __init__(self, script_dir: str, pattern: Optional[str] = None):
        """Construct a script discovery instance.

        :param script_dir: path to the directory containing migration scripts
        :param pattern: regex pattern filenames must match, defaults to ``20240315_001_description.py``
        :raises DiscoveryError: if the directory does not exist
        """
        if not os.path.isdir(script_dir):
            raise DiscoveryError(f"Migration script directory does not exist: {script_dir}")
        self._script_dir = script_dir
        self._pattern = re.compile(pattern or DEFAULT_PATTERN)

    def discover(self) -> List[MigrationScript]:
        """Scan the script directory and return matching scripts in sorted order.

        :returns: a list of ``MigrationScript`` sorted lexicographically by name
        :raises DiscoveryError: if duplicate script names are found
        """
        scripts = []
        for filename in sorted(os.listdir(self._script_dir)):
            if not self._pattern.match(filename):
                continue
            name = filename[:-3]
            path = os.path.join(self._script_dir, filename)
            scripts.append(MigrationScript(name=name, path=path))
        self._check_duplicates(scripts)
        return scripts

    @staticmethod
    def _check_duplicates(scripts: List[MigrationScript]):
        """Raise if any two scripts share the same name.

        :param scripts: the list of discovered scripts
        :raises DiscoveryError: if duplicate names are found
        """
        seen = set()
        for script in scripts:
            if script.name in seen:
                raise DiscoveryError(f"Duplicate migration script name: {script.name}")
            seen.add(script.name)
