"""
Webber: A Pythonic executor framework aimed at making parallel computing easy for everyone.
"""
import sys as _sys
import logging as _logging

__version__ = '0.1.2'
__supported__ = ("linux", "linux2", "win32")

if _sys.platform not in __supported__:
    _logging.warn(f"Webber {__version__} is only supported on these platforms: {', '.join(__supported__)}")

from .core import DAG, Condition, QueueDAG
from .xcoms import Promise
