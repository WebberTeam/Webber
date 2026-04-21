"""
Webber classes and functions related to inter-function data exchange.

Current iteration dependent on Webber tasks running on a common machine.
Uses `concurrent.futures.ThreadPoolExecutor` to maintain data as return results.

Last Updated: pre-0.0.1
"""
__all__ = ["InvalidCallable", "Promise", "IterPromise"]

import typing as _T

class InvalidCallable(Exception):
    """Requested Webber Promise is invalid in DAG's given scope/context."""
    def __init__(self, *args: _T.Any) -> None:
        super().__init__()
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self) -> str:
        if self.message:
            return f"{self.message}"
        return "InvalidCallable"


class Promise:
    """A simple object class used to handle function intercoms for Webber's DAG executor."""
    key: _T.Union[str, _T.Callable]
    def __init__(self, _key: _T.Union[str, _T.Callable]) -> None:
        """Initializing Promise using a function identifier (ID string or callable)."""
        if not callable(_key) and not isinstance(_key, str):
            err_msg = "Keys must be string IDs or callables to be assigned to a Webber Promise"
            raise TypeError(err_msg)
        self.key = _key


class IterPromise(Promise):
    """
    Signals the executor to fan-out: parent must return a list,
    child runs once per item, results aggregated for downstream.

    Single source:  IterPromise(source_a)
    Multi-source:   IterPromise(source_a, source_b, zip=True)
    """
    keys: _T.Tuple[_T.Union[str, _T.Callable], ...]
    partial: bool
    zip: bool

    def __init__(self, *_keys: _T.Union[str, _T.Callable], partial: bool = False, zip: bool = False) -> None:
        """Initialize IterPromise with one or more source keys."""
        if len(_keys) == 0:
            raise TypeError("IterPromise requires at least one source key.")
        if len(_keys) > 1 and not zip:
            raise TypeError("Multiple source keys require zip=True.")
        for k in _keys:
            if not callable(k) and not isinstance(k, str):
                raise TypeError("Keys must be string IDs or callables to be assigned to a Webber IterPromise")
        self.key = _keys[0]
        self.keys = tuple(_keys)
        self.partial = partial
        self.zip = zip
