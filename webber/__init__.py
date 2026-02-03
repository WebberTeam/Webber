"""
Webber: A Pythonic executor framework aimed at making parallel computing easy for everyone.
"""
import sys as _sys
import logging as _logging
import threading as _threading

__version__ = '0.3.0'
__supported__ = ("linux", "linux2", "win32")

if _sys.platform not in __supported__:
    _logging.warning(f"Webber {__version__} is only supported on these platforms: {', '.join(__supported__)}")

from .core import DAG, Condition, QueueDAG
from .xcoms import Promise

# Event to signal when visualization libraries are ready (or failed gracefully)
_viz_ready = _threading.Event()


def _preload_viz() -> None:
    """Background pre-import of visualization libraries to avoid circular imports."""
    try:
        import matplotlib
        # Only set backend if pyplot hasn't been imported yet
        if 'matplotlib.pyplot' not in _sys.modules:
            matplotlib.use('Agg')
        import matplotlib.pyplot
        import netgraph
    except (ImportError, AttributeError, Exception):
        # Preload failed - visualization will import on demand instead
        pass
    finally:
        # Signal ready regardless of success
        _viz_ready.set()


def wait_for_viz_ready(timeout: float = 5.0) -> bool:
    """
    Wait for visualization libraries to finish loading.

    Called internally by visualization functions to avoid race conditions.
    Does not block DAG execution - only affects visualize() calls.

    Args:
        timeout: Maximum seconds to wait (default 5.0)

    Returns:
        True if ready, False if timeout occurred
    """
    return _viz_ready.wait(timeout=timeout)


# Start background import on module load (daemon thread won't block exit)
_threading.Thread(target=_preload_viz, daemon=True, name='webber-viz-preload').start()
