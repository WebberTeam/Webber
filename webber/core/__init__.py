"""
Base module for abstract multiprocessing system - a directed acyclic graph.
"""
import sys as _sys
import typing as _T
import types as _types
import traceback as _traceback
import collections.abc as _abc
import concurrent.futures as _futures
import queue as _q
import itertools as _it
import threading as _threading
from datetime import datetime as _datetime

import networkx as _nx
import webber.edges as _edges
import webber.xcoms as _xcoms
import webber.queue as _queue

from webber.edges import Condition, dotdict, edgedict

__all__ = ["DAG", "Condition", "QueueDAG"]

def _iscallable(function: _T.Any) -> bool:
    return callable(function)


class _OutputLogger:
    """
    Thread-safe logger that uses a queue to serialize stdout writes.
    All threads put messages into a queue, a single consumer writes to stdout.
    This avoids race conditions from concurrent redirect_stdout calls.
    """
    _instance: _T.Optional['_OutputLogger'] = None
    _lock = _threading.Lock()

    def __new__(cls, *args: _T.Any, **kwargs: _T.Any) -> '_OutputLogger':
        """Singleton pattern - one logger for all threads."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, file: _T.TextIO = _sys.stdout) -> None:
        if self._initialized:
            return

        self._queue: _q.Queue[_T.Optional[str]] = _q.Queue()
        self._file = file
        self._running = True

        # Start consumer thread
        self._consumer = _threading.Thread(target=self._consume, daemon=True)
        self._consumer.start()
        self._initialized = True

    def _consume(self) -> None:
        """Consumer thread - reads from queue and writes to stdout."""
        while self._running or not self._queue.empty():
            try:
                record = self._queue.get(timeout=0.1)
                if record is None:  # Shutdown signal
                    self._queue.task_done()
                    break
                self._file.write(record + "\n")
                self._file.flush()
                self._queue.task_done()
            except _q.Empty:
                continue

    def log(self, name: str, message: str) -> None:
        """Thread-safe log - puts message on queue."""
        if message and not message.isspace():
            timestamp = _datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
            formatted = f"{timestamp} {name:>15}: {message}"
            self._queue.put(formatted)

    def shutdown(self) -> None:
        """Wait for queue to drain and stop consumer."""
        self._queue.join()
        self._running = False
        self._queue.put(None)
        self._consumer.join(timeout=1.0)

    @classmethod
    def reset(cls) -> None:
        """Reset singleton instance (useful for testing)."""
        with cls._lock:
            if cls._instance is not None:
                cls._instance.shutdown()
                cls._instance = None


class _PrefixedStdout:
    """
    Stdout wrapper that prefixes each line with timestamp and task name.
    Thread-safe via thread-local task name storage.
    """
    _local = _threading.local()

    def __init__(self, original_stdout: _T.TextIO) -> None:
        self._original = original_stdout
        self._lock = _threading.Lock()

    @classmethod
    def set_task_name(cls, name: str | None) -> None:
        """Set the task name for the current thread."""
        cls._local.task_name = name

    @classmethod
    def get_task_name(cls) -> str | None:
        """Get the task name for the current thread."""
        return getattr(cls._local, 'task_name', None)

    def write(self, msg: str) -> int:
        """Write with timestamp/task prefix if task is set."""
        task_name = self.get_task_name()
        if task_name:
            # When in a task context, suppress standalone newlines/whitespace
            # (print() sends content and newline as separate write calls)
            if not msg or not msg.strip():
                return len(msg) if msg else 0
            timestamp = _datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
            # Handle multi-line messages
            lines = [line for line in msg.rstrip('\n').split('\n') if line.strip()]
            if lines:
                formatted_lines = [f"{timestamp} {task_name:>15}: {line}" for line in lines]
                formatted = '\n'.join(formatted_lines) + '\n'
                with self._lock:
                    return self._original.write(formatted)
            return len(msg)
        else:
            with self._lock:
                return self._original.write(msg)

    def flush(self) -> None:
        self._original.flush()

    def __getattr__(self, name: str) -> _T.Any:
        return getattr(self._original, name)


# Install prefixed stdout globally (once)
_original_stdout = _sys.stdout
_prefixed_stdout = _PrefixedStdout(_original_stdout)


class _TaskLogger:
    """
    Per-task logger context manager that sets up stdout prefixing.
    Used as a context manager for each task execution.
    """
    def __init__(self, task_name: str) -> None:
        self._name = task_name
        self._prev_name: str | None = None

    def __enter__(self) -> '_TaskLogger':
        self._prev_name = _PrefixedStdout.get_task_name()
        _PrefixedStdout.set_task_name(self._name)
        _sys.stdout = _prefixed_stdout
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: _types.TracebackType | None
    ) -> bool | None:
        _PrefixedStdout.set_task_name(self._prev_name)
        if self._prev_name is None:
            _sys.stdout = _original_stdout
        return None


def _event_wrapper(
    _callable: _T.Callable,
    _name: str,
    _args: _T.Union[tuple, list],
    _kwargs: _T.Dict[str, _T.Any],
    _verbose: bool = True
) -> _T.Any:
    """Wrapper used by Webber DAGs to log and execute Python callables as a unit of work."""
    if _verbose:
        with _TaskLogger(_name):
            # print(f"Starting {_name}", flush=True)
            result = _callable(*_args, **_kwargs)
            # print(f"Completed {_name}", flush=True)
            return result
    else:
        return _callable(*_args, **_kwargs)

class DAG:
    """
    Directed Acyclic Graph used to represent Pythonic tasks in parallel.
    """
    graph: _nx.DiGraph
    _callable_to_id: _T.Dict[_T.Callable, _T.Optional[str]]  # O(1) callable->node_id lookup cache (None = duplicates)

    def _callable_status(self, c: _T.Callable) -> _T.Literal['none', 'one', 'many']:
        """Returns 'none' if not in DAG, 'one' if unique, 'many' if duplicated.
        O(1) lookup using _callable_to_id cache."""
        if c not in self._callable_to_id:
            return 'none'
        return 'one' if self._callable_to_id[c] is not None else 'many'

    def _assign_node(self, n: _T.Any, new_callables: _T.Dict[_T.Callable, _T.Callable]) -> str:
        """Helper to resolve node to its ID, adding to new_callables dict if needed."""
        if not _iscallable(n):
            return n
        status = self._callable_status(n)
        if status == 'many':
            err_msg = f"Callable {n.__name__} " \
                + "exists more than once in this DAG. " \
                + "Use the unique string identifier of the required node."
            raise ValueError(err_msg)
        if status == 'one':
            return self._callable_to_id[n]  # type: ignore[return-value]
        new_callables[n] = n
        return _edges.label_node(n)

    def add_node(self, node: _T.Any, *args: _T.Any, **kwargs: _T.Any) -> str:
        """
        Adds a callable with positional and keyword arguments to the DAG's underlying graph.
        On success, return unique identifier for the new node.
        """
        if not _iscallable(node):
            err_msg = f"{node}: requested node is not a callable Python function."
            raise TypeError(err_msg)

        node_name = _edges.label_node(node)

        args = tuple(
            arg if not isinstance(arg, _xcoms.Promise) else self.resolve_promise(arg)
            for arg in args
        )

        for k, val in kwargs.items():
            if isinstance(val, _xcoms.Promise):
                kwargs[k] = self.resolve_promise(val)

        self.graph.add_node(
            node_for_adding=node_name,
            callable=node, args=args, kwargs=kwargs,
            name=node.__name__,
            id=node_name
        )

        # Populate callable->id cache for O(1) lookup
        # If callable already exists, mark as None to indicate duplicates
        if node in self._callable_to_id:
            self._callable_to_id[node] = None  # Multiple occurrences - must use string ID
        else:
            self._callable_to_id[node] = node_name

        return node_name


    def add_edge(
            self, 
            u_of_edge: _T.Union[str, _T.Callable], v_of_edge: _T.Union[str, _T.Callable],
            continue_on: Condition = Condition.Success
        ) -> _T.Tuple[str,str]:
        """
        Adds an edge between nodes in the DAG's underlying graph,
        so long as the requested edge is unique and has not been added previously.

        On success, returns Tuple of the new edge's unique identifiers.
        """
        # Validate inputs prior to execution
        # - Nodes must be identifiers or callables
        # - Conditions must belong to the webber.edges.Condition class
        if not (isinstance(u_of_edge,str) or _iscallable(u_of_edge)):
            err_msg = f"Outgoing node {u_of_edge} must be a string or a Python callable"
            raise TypeError(err_msg)
        if not (isinstance(v_of_edge,str) or _iscallable(v_of_edge)):
            err_msg = f"Outgoing node {v_of_edge} must be a string or a Python callable"
            raise TypeError(err_msg)
        if not isinstance(continue_on, Condition):
            raise TypeError("Edge conditions must use the webber.edges.Condition class.")

        # Base Case 0: No nodes are present in the DAG:
        # Ensure that both nodes are callables, then add both to the graph and
        # assign the outgoing node as a root.
        if len(self.graph.nodes()) == 0:
            if not _iscallable(u_of_edge):
                err_msg = f"Outgoing node {u_of_edge} is not defined in this DAG's scope."
                raise ValueError(err_msg)
            if not _iscallable(v_of_edge):
                err_msg = f"Incoming node {v_of_edge} is not defined in this DAG's scope."
                raise ValueError(err_msg)
            outgoing_node = self.add_node(u_of_edge)
            incoming_node = self.add_node(v_of_edge)
            self.graph.add_edge(outgoing_node, incoming_node, Condition = continue_on)
            return (outgoing_node, incoming_node)

        new_callables: _T.Dict[_T.Callable, _T.Callable] = {}

        if _iscallable(u_of_edge) and _iscallable(v_of_edge):
            # Type narrowing: we've verified both are callables
            u_callable = _T.cast(_T.Callable, u_of_edge)
            v_callable = _T.cast(_T.Callable, v_of_edge)

            # Error Cases 0, 1: Either of the callables appear more than once in the DAG.
            if self._callable_status(u_callable) == 'many':
                err_msg = f"Callable {u_callable.__name__} " \
                        + "exists more than once in this DAG. " \
                        + "Use the unique string identifier of the required node."
                raise ValueError(err_msg)
            if self._callable_status(v_callable) == 'many':
                err_msg = f"Callable {v_callable.__name__} " \
                        + "exists more than once in this DAG. " \
                        + "Use the unique string identifier of the required node."
                raise ValueError(err_msg)

            # Base Case 1: Both args are callables and will be present in the DAG scope no more than once.
            # We will create new nodes if necessary, after validation, and get the unique string identifiers of the nodes.
            cached_u = self._callable_to_id.get(u_callable)
            if cached_u is not None:
                outgoing_node = cached_u
            else:
                new_callables[u_callable] = u_callable
                outgoing_node = _edges.label_node(u_callable)

            cached_v = self._callable_to_id.get(v_callable)
            if cached_v is not None:
                incoming_node = cached_v
            else:
                new_callables[v_callable] = v_callable
                incoming_node = _edges.label_node(v_callable)

        else:

            # Error Cases 2, 3: Either of the requested IDs are not in the DAG's current scope.
            if isinstance(u_of_edge, str) and u_of_edge not in self.graph.nodes:
                err_msg = f"Outgoing node {u_of_edge} not in DAG's current scope."
                raise ValueError(err_msg)
            if isinstance(v_of_edge, str) and v_of_edge not in self.graph.nodes:
                err_msg = f"Incoming node {v_of_edge} not in DAG's current scope."
                raise ValueError(err_msg)

            # Both nodes' unique identifiers are present in the DAG
            # and should be evaluated for a valid edge.
            if isinstance(u_of_edge, str) and isinstance(v_of_edge, str):
                outgoing_node = u_of_edge
                incoming_node = v_of_edge

            # Otherwise, one of the nodes is a callable, and the other is a valid unique identifier.
            else:
                for node in (u_of_edge, v_of_edge):
                    if node == u_of_edge:
                        outgoing_node = self._assign_node(node, new_callables)
                    else:
                        incoming_node = self._assign_node(node, new_callables)

        # Error Case 5: Both callables exist only once in the DAG,
        # but an edge already exists between them. O(1) lookup with has_edge().
        if self.graph.has_edge(outgoing_node, incoming_node):
            err_msg = f"Requested edge ({outgoing_node}, {incoming_node}) already has " \
                    + "a definition in this DAG."
            raise ValueError(err_msg)

        # Error Case 6: Adding an edge would create circular dependencies.
        # Use has_path() for O(V+E) incremental check instead of rebuilding entire graph.
        if incoming_node in self.graph and _nx.has_path(self.graph, incoming_node, outgoing_node):
            err_msg = f"Requested edge ({outgoing_node}, {incoming_node}) " \
                    + "results in circular dependencies."
            raise ValueError(err_msg)

        # We can now add the edge to the DAG, since we are certain it will not result in
        # illegal dependencies/behavior.
        # First, we should account for potential new nodes. This also handles
        # duplicates on first entry to the DAG (e.g.: edge == (print, print))
        if _iscallable(u_of_edge) and new_callables.get(_T.cast(_T.Callable, u_of_edge)) is not None:
            outgoing_node = self.add_node(new_callables[_T.cast(_T.Callable, u_of_edge)])
        if _iscallable(v_of_edge) and new_callables.get(_T.cast(_T.Callable, v_of_edge)) is not None:
            incoming_node = self.add_node(new_callables[_T.cast(_T.Callable, v_of_edge)])

        # Then we can add the new edge.
        self.graph.add_edge(outgoing_node, incoming_node, Condition = continue_on)
        return (outgoing_node, incoming_node)
    
    def remove_node(self, *posargs, **kwargs) -> None:
        """
        Currently out-of-scope. Node-removal can lead to unexpected behavior in a DAG.
        Throws error message and recommends safer methods.
        """
        raise NotImplementedError("Node removals can lead to unexpected behavior in a DAG without special care. Please consider using the skip_node operation or define a new DAG to achieve the same effect.")

    def remove_edge(self, u_of_edge: _T.Union[str,_T.Callable], v_of_edge: _T.Union[str,_T.Callable]) -> _T.Tuple[str,str]:
        """
        Removes an directed edge between nodes in the DAG's underlying graph.
        Throws error if the edge does not exist.
        On success, returns Tuple of the removed edge's unique identifiers.
        """
        edge_id = (self.node_id(u_of_edge), self.node_id(v_of_edge))
        if edge_id not in self.graph.edges(data = False):
            err_msg = "Requested edge does not exist in the DAG's scope"
            raise ValueError(err_msg)
        self.graph.remove_edge(edge_id[0], edge_id[1])
        return edge_id

    def update_edges(self, *E: _T.Any, continue_on: Condition | None = None, filter: _types.LambdaType | None = None, data: _T.Dict[str, _T.Any] | edgedict | None = None) -> None:
        """
        Flexible function to update properties of edges in the DAG's scope, 
        based on unique identifier(s) (e.g.: string IDs or unique callables) or a
        lambda filter using the edgedict syntax.
        
        List of nodes to update or filter argument is expected. Valid edge lists include:

        \t update_edges((node1, node2), ...)       or update_edges([(node1, node2)], ...)

        \t update_edges((node1, callable2), ...)   or update_edges([(node1, callable2)], ...)
        
        \t update_edges(edgedict),                 where isinstance(edgedict, webber.edges.edgedict) == True

        Parameters:

        > continue_on: webber.edges.Condition value to update matching edges with, 
        
        \t Execute child on success, failure, or any exit state, of the parent node

        > filter: lambda property that can be used instead of a list of edges to be updated.
        
        \t filter = (lambda e: n.parent == print or e.child == print)

        > data: If given, expects a dictionary or edgedict to update edge properties. Currently, only continue_on property should be set.

        \t data = { 'continue_on': webber.edges.Condition }

        """
        if len(E) == 0 and filter == None:
            raise ValueError("Either an array of edge IDs / edgedicts (E) or a filter must be passed to this function.")

        elif isinstance(E, dict) or isinstance(E, edgedict):
            E = [E]

        elif len(E) == 1 and isinstance(E[0], _abc.Iterable):
            try:
                _ = self.get_edges(E[0])
            except:
                E = E[0]

        if filter is not None:
            edge_ids = self.filter_edges(filter, data = False)
        else:
            if isinstance(E[0], dict) or isinstance(E[0], edgedict):
                try:
                    ids = [e['id'] for e in E]
                except KeyError:
                    err_msg = 'In dictionary form, all given edges must be follow edgedict standards.'
                    raise ValueError(err_msg)
            else:
                ids = E
            edge_ids = [self.get_edge(i[0], i[1], data=False) for i in ids]

        std_update = (continue_on is None)

        if std_update:
            for edge_id, e in zip(edge_ids, E):
                if data is not None:
                    self._update_edge(data, id = edge_id)
                else:
                    self._update_edge(e, id = edge_id)
        
        else:
            if continue_on is not None:
                if not isinstance(continue_on, Condition):
                    err_msg = f"Condition assignment must use webber.edges.Condition"
                    raise TypeError(err_msg)
                for e in edge_ids:
                    self.graph.edges[e]['Condition'] = continue_on

    def _update_edge(self, edgedict: _T.Dict[str, _T.Any], id: _T.Tuple[str, str] | None = None, force: bool = False) -> None:
        """
        Internal only. Update properties of an individual edge within a DAG's scope, 
        given a well-structured dictionary and the tuple identifier of the network edge. 
        Force argument bypasses validation, and should only be used internally.
        """
        if id is not None:
            try:
                if edgedict.get('id') and id != edgedict['id']:
                    raise ValueError(f"Given ID {id} inconsistent with dictionary identifier: {edgedict['id']}")
            except ValueError as e:
                if not force:
                    raise e
            edgedict['id'] = id

        expected_keys = ('parent', 'child', 'id', 'continue_on')
        if not set(expected_keys).issuperset(set(edgedict.keys())):
            raise ValueError(f"Expecting keys: {expected_keys}")

        if not force:
            
            e1, e2 = None, None

            if edgedict.get('id'):
                e1 = self.get_edge(edgedict['id'], data = False)

            if edgedict.get('parent') and edgedict.get('child'):
                e2 = self.get_edge(edgedict['parent'], edgedict['child'], data = False)
            else:
                e2 = e1

            if e1 != e2:
                raise ValueError('Edge vertices should not be changed using update functions.')
            
            elif e1 == None:
                raise ValueError('Requested edge was not given an identifier.')

            if edgedict.get('continue_on') and not isinstance(edgedict['continue_on'], Condition):
                err_msg = f"Condition assignment must use webber.edges.Condition"
                raise TypeError(err_msg)
                       
        edge_id = edgedict.pop('id')
        edge = {k: v for k,v in edgedict.items() if k not in ('parent', 'child', 'id')}
        self.graph.edges[(edge_id[0], edge_id[1])].update(edge)

    def relabel_node(self, node: _T.Union[str, _T.Callable], label: str) -> str:
        """
        Update the label, or name, given to a node in the DAG's scope, 
        given a Python string and a node identifier. 
        Well-structured wrapper for a common use-case of DAG.update_nodes.
        """
        node_id = self.node_id(node)
        if not isinstance(label, str) or len(label) == 0:
            err_msg = "Node label must be a Python string with one or more characters."
            raise ValueError(err_msg)
        self.update_nodes(node_id, data = {'name': label})
        return label

    def update_nodes(self, *N: _T.Any, filter: _types.LambdaType | None = None, data: _T.Dict[str, _T.Any] | dotdict | None = None, callable: _T.Callable | None = None, args: _T.Iterable[_T.Any] | None = None, kwargs: _T.Dict[str, _T.Any] | None = None) -> None:
        """
        Flexible function to update properties of nodes in the DAG's scope, 
        based on unique identifier(s) (e.g.: string IDs or unique callables) or a
        lambda filter using the dotdict syntax.
        
        List of nodes to update or filter argument is expected. Valid node lists include:

        \t update_nodes(node_id, ...)           or update_nodes([node_id], ...)

        \t update_nodes(callable, ...)          or update_nodes([callable], ...)
        
        \t update_nodes(node1, node2, ...)      or update_nodes([node1, node2], ...)
        
        \t update_nodes(node1, callable2, ...)  or update_nodes([node1, callable2], ...)

        > update_nodes(node_id, ...) is equivalent to update_nodes(filter = lambda n: n.id == node_id)

        Parameters:

        > filter: lambda property that can be used instead of a list of nodes to be updated.
        
        \t filter = (lambda n: n.callable == print or 'Hello, World' in n.args)

        > data: If given, expects a dictionary or dotdict to update node properties. At least one property should be defined if data argument is set.
            Any value given to the id key will be ignored. Allowed for ease of use with DAG.get nodes method.
            
        \t data = {
            \t 'callable': print,
            \t 'args': ['Hello', 'World'],
            \t 'kwargs': {'sep': ', '},
            \t 'name': 'custom_label',
            \t 'id': 'unique-identifier'
            \t}

        > args: Positional arguments to be passed to matching callables in the DAG's scope, using a Python iterable (e.g.: Tuple or List).

        > kwargs: Keyword arguments to be passed to matching callables in the DAG's scope, using a Python dictionary.

        """
        if len(N) == 0 and filter == None:
            raise ValueError("Either an array of node IDs or node data (N) or a filter must be passed to this function.")

        elif len(N) > 0 and filter is not None:
            raise ValueError("Node data array (N) and filter argument are mutually exclusive, and cannot both be defined to identify nodes to update DAG's scope.")

        elif isinstance(N, dict) or isinstance(N, str):
            N = [N]

        elif len(N) == 1 and isinstance(N[0], _abc.Iterable):
            if isinstance(N[0][0], dict):
                N = N[0]
            elif isinstance(N[0][0], str):
                # BUG: A list of all single character IDs will fail to be updated. Please try another call method (i.e.: nested iterator).
                if sum(list(map(lambda n: len(n), N[0]))) != len(N[0]): 
                    N = N[0]


        if filter is not None:
            node_ids = self.filter_nodes(filter, data = False)
        else:
            if isinstance(N[0], dict):
                ids = [n['id'] for n in N]
            else:
                ids = N
            node_ids = [self.node_id(i) for i in ids]
        
        std_update = (callable == None) and (args == None) and (kwargs == None)

        if std_update:
            for node_id, n in zip(node_ids, N):
                if data is not None:
                    self._update_node(data, id = node_id)
                else:
                    self._update_node(n, id = node_id)
        
        else:
            if callable is not None:
                if not _iscallable(callable):
                    err_msg = f"Requested node is not assigned a callable Python function."
                    raise TypeError(err_msg)
                for node_id in node_ids:
                    self.graph.nodes[node_id]['callable'] = callable
                    self.graph.nodes[node_id]['name'] = callable.__name__
            
            if args is not None:
                if not (isinstance(args, _abc.Iterable) and not isinstance(args, str)):
                    err_msg = f"Requested node is not assigned a tuple of pos args."
                    raise TypeError(err_msg)
                args = tuple(args)
                for node_id in node_ids:
                    self.graph.nodes[node_id]['args'] = args
            
            if kwargs is not None:
                if not isinstance(kwargs, dict):
                    err_msg = f"Requested node is not assigned a dictionary of kw args."
                    raise TypeError(err_msg)
                for node_id in node_ids:
                    self.graph.nodes[node_id]['kwargs'] = kwargs

    def get_edges(self, *N: _T.Any, data: bool = True) -> _T.Union[_T.List[edgedict], _T.List[_T.Tuple[str, str]]]:
        """
        Retrieval function for DAG edge data, based on tuple identifiers.
        Use filter_edges for more flexible controls (e.g.: filter_edges(in=['node_1', 'node_2']))
        """
        if len(N) == 0:
            if data:
                return [edgedict(u, v, **d) for u, v, d in self.graph.edges.data()]
            return list(self.graph.edges.data(data=False))
            
        # elif len(N) == 1:
        #     if isinstance(N[0], _abc.Iterable) and not isinstance(N[0], tuple):
        #         N = N[0]

        if len(N) != len(set(N)) or not all(isinstance(n, _abc.Iterable) and len(n) == 2 for n in N):
            err_msg = 'All requested edges must be unique tuples of size 2.'
            raise ValueError(err_msg)
    
        edge_data = [self.get_edge(o, i, data=data) for (o, i) in N]
        return edge_data
    
    def get_edge(self, outgoing_node: _T.Union[str, _T.Callable], incoming_node: _T.Union[str, _T.Callable], data: bool = True) -> _T.Union[edgedict, _T.Tuple[str, str]]:
        """
        Retrieval function for a single directed edge between nodes in a DAG's scope. 
        """
        id = (self.node_id(outgoing_node), self.node_id(incoming_node))
        if not data:
            return id
        edge_data = self.graph.get_edge_data(u = id[0], v = id[1])
        if not edge_data:
            err_msg = f'No match found for the directed edge requested: {id}'
            raise ValueError(err_msg)
        assert edge_data is not None  # Type narrowing for Pylance
        return edgedict(*id, **edge_data)

    def get_node(self, n: _T.Union[str, _T.Callable]) -> dotdict:
        """
        Given a unique identifier, returns a dictionary of node metadata
        for a single node in the DAG's scope.
        """
        node_id = self.node_id(n)
        return dotdict(self.graph.nodes[node_id])

    def get_nodes(self, *N: _T.Any) -> _T.List[dotdict]:
        """
        Flexible function to retrieve DAG node data, based on node identifiers 
        (e.g.: string IDs or unique callables).
        """
        if len(N) == 0:
            node_data = list(self.graph.nodes.values())
            return [dotdict(d) for d in node_data]

        elif len(N) == 1:
            if isinstance(N[0], _abc.Iterable) and not isinstance(N[0], str):
                N = N[0]
            else:
                node_id = self.node_id(N[0])
                node_data = [dotdict(self.graph.nodes[node_id])]
                return node_data
            
        if not len(N) == len(set(N)):
            err_msg = 'All requested nodes must be unique identifiers.'
            raise ValueError(err_msg)
        
        node_ids  = [self.node_id(n) for n in N]
        node_data = [dotdict(self.graph.nodes[n]) for n in node_ids]
        return node_data

    def filter_nodes(self, filter: _types.LambdaType, data: bool = False) -> _T.List[str] | _T.List[dotdict]:
        """
        Given a lambda function, filter nodes in a DAG's scope based on its attributes.
        Current limitation: Filters must use node identifier strings when referencing nodes.
        Use get_nodes for more flexible controls.
        """
        if not data:
            return [node['id'] for node in self.graph.nodes.values() if filter(dotdict(node))]
        return [dotdict(node) for node in self.graph.nodes.values() if filter(dotdict(node))]
    
    def filter_edges(self, filter: _types.LambdaType, data: bool = False) -> _T.List[_T.Tuple[str, str]] | _T.List[edgedict]:
        """
        Given a lambda function, filter edges in a DAG's scope based on its attributes.
        Current limitation: Filters must use node identifier strings when referencing nodes.
        Use get_edges for more flexible controls.
        """
        if not data:
            return [e[:2] for e in list(self.graph.edges.data()) if filter(edgedict(*e))]
        return [edgedict(*e) for e in list(self.graph.edges.data()) if filter(edgedict(*e))]

    def retry_node(self, identifier: _T.Union[str,_T.Callable], count: int) -> None:
        """
        Given a node identifier, set number of automatic retries in case of failure.
        Re-attempts will begin as soon as possible.
        """
        if not isinstance(count, int) or not count >= 0:
            raise ValueError("Retry count must be a non-negative integer.")
        node = self.node_id(identifier)
        self.graph.nodes[node]['retry'] = count

    def skip_node(self, identifier: _T.Union[str, _T.Callable], skip: bool = True, as_failure: bool = False) -> None:
        """
        Given a node identifier, set DAG to skip node execution as a success (stdout print) or a failure (exception error).
        Allows conditional control and testing over DAG's order of operations.
        """
        if not isinstance(skip, bool):
            raise ValueError("Skip argument must be a boolean value.")
        node = self.node_id(identifier)
        self.graph.nodes[node]['skip'] = (skip, as_failure)
      
    def critical_path(self, nodes: _T.Union[str, _T.Callable, _T.Iterable[_T.Union[str, _T.Callable]]]) -> 'DAG':
        """
        Given a set of nodes, returns a subset of the DAG containing
        only the node(s) and its parents, or upstream dependencies.
        """
        if isinstance(nodes, _abc.Iterable) and not isinstance(nodes, str):
            node_ids = {self.node_id(n) for n in nodes}
        else:
            node_ids = {self.node_id(nodes)}
        return self._subgraph(node_ids)

    def execute(
        self,
        return_ref: bool = False,
        print_exc: bool = False,
        max_workers: _T.Optional[int] = None,
        verbose: bool = True
    ) -> _T.Optional['DAG.DAGExecutor']:
        """
        Basic wrapper for execution of the DAG's underlying callables.

        Args:
            return_ref: If True, return the DAGExecutor instance.
            print_exc: If True, print full exception tracebacks.
            max_workers: Maximum number of worker threads. Defaults to None (auto).
            verbose: If True, log task start/completion messages. Defaults to True.
        """
        executor = self.DAGExecutor(
            self.graph, self.root, print_exc,
            max_workers=max_workers, verbose=verbose
        )
        return executor if return_ref else None

    def visualize(self, type: _T.Literal['gui', 'browser', 'plt'] | None = None) -> _T.Any:
        """
        Basic wrapper to visualize DAG using Vis.js and NetGraph libraries.
        By default, visualization library only loaded in after DAG.visualize() is called, halving import times.
        """
        import webber.viz as _viz

        match type:
            case 'browser':
                _viz.visualize_browser(self.graph)

            case 'plt':
                return _viz.visualize_plt(self.graph)

            case 'gui':
                # _visualize_gui(self.graph)
                raise NotImplementedError

            case None: 
                if _viz._in_notebook():
                    return _viz.visualize_plt(self.graph)
                else:
                    _viz.visualize_browser(self.graph)

            case _:
                err_msg = "Unknown visualization type requested."
                raise NotImplementedError(err_msg)

    @property
    def root(self) -> _T.List[str]:
        """
        Return list of nodes with no dependencies.
        Root nodes will occur first in DAG's order of operations.
        Uses O(1) in_degree() instead of O(k) predecessors list creation.
        """
        return [node for node in self.graph.nodes if self.graph.in_degree(node) == 0]

    @property
    def nodes(self) -> _T.Any:
        return self.graph.nodes

    def node_id(self, identifier: _T.Union[str,_T.Callable]) -> str:
        """
        Validate whether identifier given is a valid node within the DAG's scope.
        Primarily for internal use, but useful for retrieving string identifiers
        for a unique callable in a DAG.
        Uses O(1) cache lookup for callables instead of O(n) linear search.
        """
        if isinstance(identifier, str):
            # O(1) lookup using graph.nodes dict
            if identifier not in self.graph.nodes:
                err_msg = f"Node {identifier} is not defined in this DAG's scope."
                raise ValueError(err_msg)
            return identifier
        elif _iscallable(identifier):
            # O(1) lookup using _callable_to_id cache
            if identifier not in self._callable_to_id:
                err_msg = f"Callable {identifier} is not defined in this DAG's scope."
                raise ValueError(err_msg)
            cached_id = self._callable_to_id[identifier]
            if cached_id is None:
                err_msg = f"Callable {identifier.__name__} " \
                        + "exists more than once in this DAG. " \
                        + "Use the unique string identifier of the required node."
                raise ValueError(err_msg)
            return cached_id
        else:
            err_msg = f"Node {identifier} must be a string or a Python callable"
            raise TypeError(err_msg)
    
    def _update_node(self, nodedict: _T.Dict[str, _T.Any], id: str | None = None, force: bool = False) -> None:
        """
        Internal only. Update properties of single node within a DAG's scope, 
        given a well-structured dictionary and the tuple identifier of the network edge. 
        Force argument bypasses dictionary validation, and should only be used internally.
        """
        if id is not None:
            try:
                if nodedict.get('id') is not None and id != nodedict['id']:
                    raise ValueError(f"Given ID {id} inconsistent with dictionary identifier: {nodedict['id']}")
            except ValueError as e:
                if not force:
                    raise e
            nodedict['id'] = id
        
        expected_keys = ('callable', 'args', 'kwargs', 'name', 'id')
        if not set(expected_keys).issuperset(set(nodedict.keys())):
            raise ValueError(f"Expecting keys: {expected_keys}")
        
        if not force:
            if nodedict.get('callable'):
                if not _iscallable(nodedict['callable']):
                    err_msg = f"Requested node is not assigned a callable Python function."
                    raise TypeError(err_msg)
                if not nodedict.get('name'):
                    nodedict['name'] = nodedict['callable'].__name__

            if nodedict.get('name') and (not isinstance(nodedict['name'], str) or len(nodedict['name']) == 0): 
                err_msg = f"Requested node name must be a non-null Python string, will default to callable when not set."
                raise TypeError(err_msg)

            if nodedict.get('args'):
                if not (isinstance(nodedict['args'], _abc.Iterable) and not isinstance(nodedict['args'], str)):
                    err_msg = f"Requested node is not assigned a tuple of pos args."
                    raise TypeError(err_msg)
                nodedict['args'] = tuple(nodedict['args'])
            
            if nodedict.get('kwargs') and not isinstance(nodedict['kwargs'], dict):
                err_msg = f"Requested node is not assigned a dictionary of kw args."
                raise TypeError(err_msg)                    
        
        node_id = nodedict.pop('id')
        self.graph.nodes[node_id].update(nodedict)

        # Reset node name if implicitly requested.
        if not nodedict.get('name'):
            self.graph.nodes[node_id]['name'] = self.graph.nodes[node_id]['callable'].__name__

    def _subgraph(self, node_ids: set[str]) -> 'DAG':
        """
        Internal only. Given a set of nodes, returns a subset of the DAG containing
        only the node(s) and upstream dependencies.
        Uses nx.ancestors() for O(V+E) performance instead of manual traversal.
        """
        all_nodes = set(node_ids)
        for node in node_ids:
            all_nodes.update(_nx.ancestors(self.graph, node))
        subgraph = self.graph.subgraph(all_nodes)
        return DAG(subgraph, __force=True)

    def resolve_promise(self, promise: _xcoms.Promise) -> _xcoms.Promise:
        """
        Returns a Promise with a unique string identifier, if a given Promise is valid, based on the DAG's current scope.
        Raises `webber.xcoms.InvalidCallable` if Promise requests a callable that is out of scope.
        """
        try:
            key = self.node_id(promise.key)
        except Exception as e:
            raise _xcoms.InvalidCallable(e)
        return _xcoms.Promise(key)

    def __init__(self, graph: _T.Union[_nx.DiGraph, _nx.Graph, None] = None, **kwargs: _T.Any) -> None:

        if graph is None:
            self.graph = _nx.DiGraph()
            self._callable_to_id = {}
            return

        # Meant for internal use only, creating DAGs from subgraphs.
        if kwargs.get('__force') == True:
            self.graph = _T.cast(_nx.DiGraph, graph)
            # Build cache from existing subgraph
            self._callable_to_id = {}
            for node_id, data in self.graph.nodes(data=True):
                callable_fn = data.get('callable')
                if callable_fn is not None:
                    if callable_fn in self._callable_to_id:
                        self._callable_to_id[callable_fn] = None  # Duplicate
                    else:
                        self._callable_to_id[callable_fn] = node_id
            return

        _edges.validate_dag(graph)
        # Type narrowing: validate_dag ensures graph is a DiGraph
        assert isinstance(graph, _nx.DiGraph)

        # Define framework specific logic as nested dictionaries.
        for node in graph.nodes.keys():
            graph.nodes[node]['callable'] = node
            graph.nodes[node]['name'] = node.__name__
            graph.nodes[node]['args'] = []
            graph.nodes[node]['kwargs'] = {}

        for e in graph.edges:
            condition = graph.edges[e].get('Condition')
            if condition is not None:
                if condition not in Condition:
                    raise TypeError(e, 'Edge conditions must belong to IntEnum type Webber.Condition.')
            else:
                graph.edges[e]['Condition'] = Condition.Success

        graph = _nx.relabel_nodes(graph, lambda node: _edges.label_node(node))
        for n in graph.nodes:
            graph.nodes[n]['id'] = n
        self.graph = _nx.DiGraph(graph)

        # Build callable->id cache after relabeling
        self._callable_to_id = {}
        for node_id, data in self.graph.nodes(data=True):
            callable_fn = data.get('callable')
            if callable_fn is not None:
                if callable_fn in self._callable_to_id:
                    self._callable_to_id[callable_fn] = None  # Duplicate
                else:
                    self._callable_to_id[callable_fn] = node_id

    class DAGExecutor:
        """
        Base class used to execute DAG in embarrassingly parallel.
        """
        def __init__(
            self,
            graph: _nx.DiGraph,
            roots: _T.List[str],
            print_exc: bool = False,
            max_workers: _T.Optional[int] = None,
            verbose: bool = True
        ) -> None:

            # Skip execution if there are no callables in scope.
            if len(graph.nodes) == 0:
                if verbose:
                    print('Given DAG has no callables in scope. Skipping execution...')
                return

            # Initialize local variables for execution.
            complete: set[str] = set()
            started: set[str] = set()
            failed: set[str] = set()
            skipped: set[str] = set()
            refs: _T.Dict[str, _futures.Future[_T.Any]] = {}

            def raise_exc(message: str) -> None:
                raise ValueError(message)

            def run_conditions_met(n: str) -> bool:
                for p in graph.predecessors(n):
                    match graph.edges[(p, n)]['Condition']:
                        case Condition.Success:
                            if p not in complete:
                                return False
                        case Condition.Failure:
                            if p not in failed:
                                return False
                        case Condition.AnyCase:
                            if p not in failed and p not in complete:
                                return False
                return True

            skip = graph.nodes.data("skip", default=(False, False))
            retry: _T.Dict[str, _T.List[_T.Any]] = {
                n: [c + 1, {}] for n, c in graph.nodes.data("retry", default=0)
            }

            # Start execution of root node functions.
            with _futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

                def Submit(
                    event: str,
                    callable: _T.Callable[..., _T.Any],
                    name: str,
                    args: _T.Union[tuple[_T.Any, ...], list[_T.Any]],
                    kwargs: _T.Dict[str, _T.Any]
                ) -> _futures.Future[_T.Any]:
                    if skip[event][0]:
                        retry[event][0] = 0
                        skip_callable = raise_exc if skip[event][1] else print
                        return executor.submit(
                            _event_wrapper,
                            _callable=skip_callable,
                            _name=graph.nodes[event]['name'],
                            _args=[f"Event {event} skipped..."],
                            _kwargs={},
                            _verbose=verbose
                        )
                    else:
                        retry[event][0] -= 1
                        if (retry[event][0] > 0) and (retry[event][1] == {}):
                            retry[event][1] = {
                                'callable': callable,
                                'name': name,
                                'args': args,
                                'kwargs': kwargs
                            }
                        return executor.submit(
                            _event_wrapper,
                            _callable=callable,
                            _name=name,
                            _args=args,
                            _kwargs=kwargs,
                            _verbose=verbose
                        )

                # Submit root nodes
                for event in roots:
                    refs[event] = Submit(
                        event,
                        graph.nodes[event]['callable'],
                        graph.nodes[event]['name'],
                        graph.nodes[event]['args'],
                        graph.nodes[event]['kwargs']
                    )
                    started.add(event)

                # Process futures as they complete (no busy-wait)
                pending = set(refs.values())
                future_to_event = {v: k for k, v in refs.items()}

                while (len(complete) + len(failed) + len(skipped)) != len(graph):
                    if not pending:
                        break

                    # Wait for at least one future to complete
                    done, pending = _futures.wait(pending, return_when=_futures.FIRST_COMPLETED)

                    for future in done:
                        event = future_to_event[future]

                        if future.exception() is not None:
                            try:
                                raise future.exception()  # type: ignore
                            except:
                                if print_exc:
                                    _traceback.print_exc()

                                if retry[event][0] > 0:
                                    if verbose:
                                        print(f"Event {event} exited with exception, retrying...")
                                    new_future = Submit(
                                        event,
                                        callable=retry[event][1]['callable'],
                                        name=retry[event][1]['name'],
                                        args=retry[event][1]['args'],
                                        kwargs=retry[event][1]['kwargs']
                                    )
                                    refs[event] = new_future
                                    pending.add(new_future)
                                    future_to_event[new_future] = event
                                    continue

                                if verbose:
                                    print(f"Event {event} exited with exception...")
                                failed.add(event)
                                skipping = [
                                    e[1] for e in set(graph.out_edges(event))
                                    if not _edges.continue_on_failure(graph.edges[e])
                                ]
                        else:
                            complete.add(event)
                            skipping = [
                                e[1] for e in set(graph.out_edges(event))
                                if not _edges.continue_on_success(graph.edges[e])
                            ]

                        skipped = skipped.union(skipping)
                        for n in skipping:
                            skipped = skipped.union(_nx.descendants(graph, n))

                        carryon = set(graph.successors(event)).difference(skipped)
                        starting = [
                            successor for successor in carryon
                            if run_conditions_met(successor)
                        ]

                        for successor in starting:
                            _args = [
                                a if not isinstance(a, _xcoms.Promise) else refs[_T.cast(str, a.key)].result()
                                for a in graph.nodes[successor]['args']
                            ]
                            _kwargs = {
                                k: v if not isinstance(v, _xcoms.Promise) else refs[_T.cast(str, v.key)].result()
                                for k, v in graph.nodes[successor]['kwargs'].items()
                            }
                            new_future = Submit(
                                successor,
                                graph.nodes[successor]['callable'],
                                graph.nodes[successor]['name'],
                                _args,
                                _kwargs
                            )
                            refs[successor] = new_future
                            pending.add(new_future)
                            future_to_event[new_future] = successor
                            started.add(successor)

class QueueDAG(DAG):
    """
    Directed Acyclic Graph used to queue and execute Pythonic callables in parallel,
    while stringing the outputs of those callables in linear sequences.

    Queue DAG nodes are repeated until the DAG executor completes or is killed, depending on the
    behavior of root nodes to determine if and/or when the DAG run has been completed.

    Root nodes will be re-executed until culled by one of two conditions:
    1. A max number of iterations has been completed, or
    2. The output of the root node's callable matches a lambda halt_condition.

    Both conditions can be set at run-time.

    QueueDAG can be nested inside a standard webber.DAG by passing qdag.execute as a callable.
    Benchmarks show moderate overhead of 1.3-2.5x (~1-2ms fixed cost) when nested, due to the
    outer DAG's ThreadPoolExecutor setup and task wrapper infrastructure. This is well within
    acceptable millisecond-scale latency for most real-time applications.
    """

    conditions: _T.Dict[str, _T.Dict[str, _T.Any]] = {}

    def __init__(self) -> None:
        super().__init__()

    def add_node(self, node: _T.Any, *args: _T.Any, **kwargs: _T.Any) -> str:
        """
        Adds a callable with positional and keyword arguments to the DAG's underlying graph.
        On success, return unique identifier for the new node.
        
        Reserved key-words are used for Queue DAG definitions:
        
        - halt_condition: Lambda function used to halt repeated execution of a Queue DAG node that is independent of other callables.

        \t halt_condition = (lambda output: output == None) 

        - iterator: Discrete number of times that Queue DAG node should be executed. Meant to be mutually-exclusive of halt_condition argument.

        - max_iter: Maximum number of times that Queue DAG node should be executed. Meant for use with halt_condition in order to prevent forever loop.
        """
        halt_condition = kwargs.pop('halt_condition', None)
        iterator: int = kwargs.pop('iterator', None)
        max_iter: int = kwargs.pop('max_iter', None)

        return_val = super().add_node(node, *args, **kwargs)
        
        if max_iter is not None:
            iter_limit = int(max_iter)
        elif iterator is not None:
            iter_limit = int(iterator)
        else:
            iter_limit = None
        
        self.conditions[return_val] = {
            'halt_condition': halt_condition,
            'iter_limit': iter_limit
        }

        return return_val
        
    def add_edge(self, u_of_edge: _T.Union[str, _T.Callable], v_of_edge: _T.Union[str, _T.Callable], continue_on: Condition = Condition.Success) -> _T.Tuple[str, str]:
        """
        Adds an edge between nodes in the Queue DAG's underlying graph.
        Queue DAG nodes may have a maximum of one child and one parent worker.
        """
        for node in (u_of_edge, v_of_edge):
            try:
                node_id = self.node_id(node)
            except:
                continue
        
            filter = (lambda e: e.parent == node_id) if node == u_of_edge else (lambda e: e.child == node_id)
            try:
                assert(len(self.filter_edges(filter)) == 0)
            except Exception as e:
                e.add_note("Queue DAG nodes may have a maximum of one child and one parent worker.")
                raise e

        return super().add_edge(u_of_edge, v_of_edge, continue_on)

    def execute(self, *promises: _T.Any, return_ref: bool = False, print_exc: bool = False) -> _T.List[_T.Any] | None:
        """
        Basic wrapper for execution of the DAG's underlying callables.
        """
        queues: _T.Dict[str, _q.LifoQueue[_T.Any]] = {}
        processes: _T.Dict[str, _futures.Future[_T.Any]] = {}
        join: set[str] = set()
        end_proc: _T.Optional[str] = None

        _promises: _T.Dict[str, _T.Any] = { k: v for k, v in _it.pairwise(promises) } if len(promises) > 0 else {}

        with _TaskLogger("root") as _:

            # Skip execution if there are no callables in scope.
            if len(self.graph.nodes) == 0:
                print('Given DAG has no callables in scope. Skipping execution...')
                return

            with _futures.ThreadPoolExecutor() as executor:

                for id in self.root:
                    node = self.get_node(id)
                    queues[id] = _q.LifoQueue()
                    node.update({
                        'callable': _queue._worker,
                        'args': tuple(),
                        'kwargs': {
                            'work': node.callable,
                            'args': node.args, 'kwargs': node.kwargs,
                            'promises': _promises,
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
                    queues[id] = _q.LifoQueue()
                    if len(list(self.graph.successors(id))) == 0:
                        end_proc = id
                    node.update({
                        'callable': _queue._worker,
                        'args': tuple(),
                        'kwargs': {
                            'work': node.callable,
                            'args': node.args, 'kwargs': node.kwargs,
                            'promises': _promises,
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

            # For single-node DAGs with no edges, end_proc is the root node
            if end_proc is None and len(self.root) > 0:
                end_proc = self.root[0]

            while len(join) != len(self.graph.nodes):
                for node in self.graph.nodes:
                    if processes[node].done():
                        join.add(node)

            return_val: _T.List[_T.Any] = []
            if end_proc is not None and end_proc in queues:
                while not queues[end_proc].empty():
                    return_val.append(queues[end_proc].get())

            return return_val
