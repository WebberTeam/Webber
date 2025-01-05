"""
Experimenting with multiprocessing queues.
Hopeful that this will become an extension of the DAG class.
"""
import multiprocessing as mp
import uuid as _uuid
from webber.xcoms import Promise
import concurrent.futures as _futures
from webber.core import DAG, _OutputLogger, _event_wrapper
from typing import Callable
from queue import LifoQueue
import traceback as _traceback
from itertools import pairwise

def worker(work: Callable, args, kwargs: dict, promises: dict = {}, print_exc = False,
           parent_id: str = None, parent_process: _futures.Future = None, in_queue: mp.Queue = None,
           halt_condition: Callable = None, iter_limit: int = None, out_queue: mp.Queue = None):

    try:
        
        args = list(args)

        for i in range(len(args)):
            if isinstance(args[i], Promise):
                if args[i].key in promises.keys():
                    args[i] = promises[args[i].key]
        
        for k, v in kwargs.items():
            if isinstance(v, Promise):
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
                    if isinstance(args[a], Promise):
                        if args[a].key == parent_id:
                            args[a] = output
                
                for k, v in kwargs.items():
                    if isinstance(v, Promise):
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

class QueueDAG(DAG):

    conditions = {}

    def __init__(self):
        super().__init__()

    def add_node(self, node, *args, **kwargs):
        
        halt_condition = kwargs.pop('halt_condition', None)
        iterator: int = kwargs.pop('iterator', None)
        max_iter: int = kwargs.pop('max_iter', None)

        return_val = super().add_node(node, *args, **kwargs)
        
        if max_iter != None:
            iter_limit = int(max_iter)
        elif iterator != None:
            iter_limit = int(iterator)
        else:
            iter_limit = None
        
        self.conditions[return_val] = {
            'halt_condition': halt_condition,
            'iter_limit': iter_limit
        }

        return return_val
        
    # def add_edge(self, u_of_edge, v_of_edge, continue_on = Condition.Success):
    #     return super().add_edge(u_of_edge, v_of_edge, continue_on)

    def execute(self, *promises, return_ref=False, print_exc=False):
        """
        Basic wrapper for execution of the DAG's underlying callables.
        """

        queues = {}
        processes = {}
        join = set()
        
        promises: dict = { k: v for k, v in pairwise(promises) } if len(promises) > 0 else {}

        with _OutputLogger(str(_uuid.uuid4()), "INFO", "root") as _:
            with _futures.ThreadPoolExecutor() as executor:

                for id in self.root:
                    node = self.get_node(id)
                    queues[id] = LifoQueue()
                    node.update({
                        'callable': worker,
                        'args': tuple(),
                        'kwargs': {
                            'work': node.callable,
                            'args': node.args, 'kwargs': node.kwargs,
                            'promises': promises,
                            'print_exc': print_exc,
                            'halt_condition': self.conditions[id]['halt_condition'],
                            'iter_limit': self.conditions[id]['iter_limit'],
                            'out_queue': queues.get(id)
                        }
                    })
                    processes[id] = executor.submit(
                        _event_wrapper,
                        _callable=node['callable'],
                        _name=node['name'],
                        _args=node['args'],
                        _kwargs=node['kwargs']
                    )
                
                for parent_id, id in self.graph.edges:
                    node = self.get_node(id)
                    queues[id] = LifoQueue()
                    if len(list(self.graph.successors(id))) == 0:
                        endproc = id
                    node.update({
                        'callable': worker,
                        'args': tuple(),
                        'kwargs': {
                            'work': node.callable,
                            'args': node.args, 'kwargs': node.kwargs,
                            'promises': promises,
                            'print_exc': print_exc,
                            'parent_id': parent_id,
                            'parent_process': processes[parent_id],
                            'in_queue': queues.get(parent_id),
                            'out_queue': queues.get(id)
                        }
                    })
                    processes[id] = executor.submit(
                        _event_wrapper,
                        _callable=node['callable'],
                        _name=node['name'],
                        _args=node['args'],
                        _kwargs=node['kwargs']
                    )

            
            while len(join) != len(self.graph.nodes):
                for node in self.graph.nodes:
                    if processes[node].done():
                        join.add(node)
                
            return_val = []
            while not queues[endproc].empty():
                return_val.append(queues[endproc].get())
            
            return return_val
