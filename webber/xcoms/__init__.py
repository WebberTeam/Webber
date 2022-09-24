"""
Webber classes and functions related to inter-function data exchange.

Current iteration dependent on Webber tasks running on a common machine.
Uses `concurrent.futures.ThreadPoolExecutor` to maintain data as return results.

Last Updated: pre-0.0.1
"""
class InvalidCallable(Exception):
    """Requested Webber Promise is invalid in DAG's given scope/context."""
    def __init__(self, *args):
        super().__init__()
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return f"{self.message}"
        return "InvalidCallable"


class Promise:                              # pylint: disable=too-few-public-methods
    """A simple object class used to handle function intercoms for Webber's DAG executor."""
    key: str
    def __init__(self, _key: str) -> None:
        """Initializing Promise using a function ID string (`webber.DAG.add_node`)."""
        if not isinstance(_key, str):
            err_msg = "Keys must be strings given by `webber.DAG.add_node`."
            raise TypeError(err_msg)
        self.key = _key
