"""
Vdirsyncer synchronizes calendars and contacts.
"""

from __future__ import annotations

PROJECT_HOME = "https://github.com/pimutils/vdirsyncer"
BUGTRACKER_HOME = PROJECT_HOME + "/issues"
DOCS_HOME = "https://vdirsyncer.pimutils.org/en/stable"

__version__ = "0.20.0+eventix"

__all__ = ["__version__"]


def _check_python_version():
    import sys

    if sys.version_info < (3, 9, 0):  # noqa: UP036
        print("vdirsyncer requires at least Python 3.9.")
        sys.exit(1)


_check_python_version()
del _check_python_version
