"""
Webber: A Pythonic executor framework aimed at making parallel computing easy for everyone.
"""
from sys import platform as _platform
from .core import DAG
_VERSION = "0.0.1"
_supported = ["linux", "linux2"]
if _platform not in _supported:
    err_msg = f"Webber {_VERSION} is only supported on these platforms: {', '.join(_supported)}"
    raise NotImplementedError(err_msg)
