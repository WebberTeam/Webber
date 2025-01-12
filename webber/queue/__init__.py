"""
Experimenting with multiprocessing queues.
Hopeful that this will become an extension of the DAG class.
"""
import typing as _T
import queue as _q
import traceback as _traceback
import concurrent.futures as _futures
import webber.xcoms as _xcoms

__all__ = []

def _worker(work: _T.Callable, args, kwargs: dict, promises: dict = {}, print_exc = False,
           parent_id: str = None, parent_process: _futures.Future = None, in_queue: _q.LifoQueue = None,
           halt_condition: _T.Callable = None, iter_limit: int = None, out_queue: _q.LifoQueue = None):

    try:
        
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

        while iter_limit == None or (iter_count < iter_limit):
            
            # For child processes, get latest value from the queue.
            # If none are available and parent process is complete, break.
            if in_queue != None:
                try:
                    output = in_queue.get_nowait()
                except Exception as e:
                    if not in_queue.empty():
                        raise e
                    elif parent_process.done():
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
            if out_queue != None:
                out_queue.put(x)
            
            iter_count += 1
            
            # Check halt conditions for root process (output-based lambda or iteration limit).
            if halt_condition != None and halt_condition(x):
                break

    except Exception as e:
        if print_exc:
            _traceback.print_exc()
        print('Exception during runtime, ending process...')
        raise e
