"""
v0.2: Experimental implementation. Not ready for use in production contexts.
Helper class for queue-based DAG implementations.
"""
import typing as _T
import queue as _q
import traceback as _traceback
import concurrent.futures as _futures
import webber.xcoms as _xcoms

__all__ = []

def _worker(work: _T.Callable, args: _T.Tuple[_T.Any, ...] | _T.List[_T.Any], kwargs: _T.Dict[str, _T.Any],
           promises: _T.Dict[str, _T.Any] | None = None, print_exc: bool = False,
           parent_id: str | None = None, parent_process: _futures.Future[_T.Any] | None = None,
           in_queue: _q.LifoQueue[_T.Any] | None = None,
           halt_condition: _T.Callable[[_T.Any], bool] | None = None, iter_limit: int | None = None,
           out_queue: _q.LifoQueue[_T.Any] | None = None) -> None:

    try:
        # Handle mutable default
        if promises is None:
            promises = {}

        args = list(args)

        for i in range(len(args)):
            if isinstance(args[i], _xcoms.Promise):
                if args[i].key in promises.keys():
                    args[i] = promises[args[i].key]
        
        for k, v in kwargs.items():
            if isinstance(v, _xcoms.Promise):
                if v.key in promises.keys():
                    kwargs[k] = promises[v.key]

        iter_count = 0

        while iter_limit is None or (iter_count < iter_limit):
            
            # For child processes, get latest value from the queue.
            # If none are available and parent process is complete, break.
            if in_queue is not None:
                try:
                    output = in_queue.get_nowait()
                except Exception as e:
                    if not in_queue.empty():
                        raise e
                    elif parent_process is not None and parent_process.done():
                        break
                    continue
                    
                for a in range(len(args)):
                    if isinstance(args[a], _xcoms.Promise):
                        if args[a].key == parent_id:
                            args[a] = output
                
                for k, v in kwargs.items():
                    if isinstance(v, _xcoms.Promise):
                        if v.key == parent_id:
                            kwargs[k] = output

            # Execute unit of work, and push output to queue, if given.
            x = work(*args, **kwargs)
            if out_queue is not None:
                out_queue.put(x)
            
            iter_count += 1
            
            # Check halt conditions for root process (output-based lambda or iteration limit).
            if halt_condition is not None and halt_condition(x):
                break

    except Exception as e:
        if print_exc:
            _traceback.print_exc()
        print('Exception during runtime, ending process...')
        raise e
