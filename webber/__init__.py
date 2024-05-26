"""
Webber: A Pythonic executor framework aimed at making parallel computing easy for everyone.
"""
import sys as _sys

__version__ = '0.0.2'
__supported__ = ("linux", "linux2", "win32")

if _sys.platform not in __supported__:
    err_msg = f"Webber {__version__} is only supported on these platforms: {', '.join(__supported__)}"
    raise NotImplementedError(err_msg)

from .core import DAG, Condition
