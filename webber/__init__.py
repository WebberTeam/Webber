"""
Webber: A Pythonic executor framework aimed at making parallel computing easy for everyone.
"""
import sys as _sys
import logging as _logging
import threading as _threading

__version__ = '0.1.2'
__supported__ = ("linux", "linux2", "win32")

if _sys.platform not in __supported__:
    _logging.warning(f"Webber {__version__} is only supported on these platforms: {', '.join(__supported__)}")

from .core import DAG, Condition, QueueDAG
from .xcoms import Promise


def _preload_viz() -> None:
    """Background pre-import and initialization of visualization libraries to reduce first-call latency."""
    try:
        # Set non-interactive backend BEFORE importing pyplot to avoid GUI overhead
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        # Trigger font cache initialization (otherwise happens on first figure)
        plt.figure()
        plt.close()
        # Import netgraph (slow due to numpy/scipy dependencies)
        import netgraph
    except ImportError:
        pass

# Start background import on module load (daemon thread won't block exit)
_threading.Thread(target=_preload_viz, daemon=True).start()
