"""
Base module for abstract multiprocessing system - a directed acyclic graph.
"""
import sys as _sys
import typing as _T
import uuid as _uuid
import logging as _logging
import traceback as _traceback
import contextlib as _contextlib
import concurrent.futures as _futures
import networkx as _nx
import webber.edges as _edges
import webber.xcoms as _xcoms
import webber.viz as _viz

__all__ = ["DAG"]

class _OutputLogger:
    """
    Basic logger for synchronizing parallel output of Webber tasks.

    Adapted from John Paton: https://johnpaton.net/posts/redirect-logging/
    """
    def __init__(self, name="root", level="INFO", callable_name="root", file=_sys.stdout) -> None:
        """Initializes logger for given scope."""
        self.logger    = _logging.getLogger(name)
        self.name      = self.logger.name
        self.callable  = callable_name
        self.level     = getattr(_logging, level)
        self.format    = _logging.Formatter(
                            "{asctime} " + f"{self.callable:>15}:" + " {message}",
                            style="{",
                        )
        stream_handler = _logging.StreamHandler(file)
        stream_handler.setFormatter(self.format)
        self.logger.addHandler(stream_handler)
        self.logger.setLevel(self.level)
        self._redirector = _contextlib.redirect_stdout(self)
        # elif file is _stderr:
        #     self._redirector = _contextlib.redirect_stderr(self)

    def write(self, msg: str):
        """Writes non-empty strings to the logger's output -- stdout, by default."""
        if msg and not msg.isspace():
            self.logger.log(self.level, msg)

    def flush(self):
        """Ignores flushing of output, since logs are not concerned with such characters. (?)"""
        pass # pylint: disable=unnecessary-pass

    def __enter__(self):
        """Allows Python built-ins for printing to forward data in scope. (?)"""
        self._redirector.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Allows Python built-ins to exit scope on error. (?)"""
        self._redirector.__exit__(exc_type, exc_value, traceback)

def _event_wrapper(_callable: callable, _name: str, _args, _kwargs):
    with _OutputLogger(str(_uuid.uuid4()), "INFO", _name) as _:
        return _callable(*_args, **_kwargs)

class DAG:
    """
    Directed Acyclic Graph used to represent Pythonic tasks in parallel.
    """
    @property
    def root(self):
        return list(filter(
            lambda node: len(list(self.graph.predecessors(node))) < 1,
            self.graph.nodes.keys()
        ))
    
    class DAGExecutor: # pylint: disable=too-few-public-methods,too-many-locals
        """
        Base class used to execute DAG in embarrassingly parallel.
        """
        def __init__(self, graph: _nx.DiGraph, roots: list) -> None:
            with _OutputLogger(str(_uuid.uuid1()), "INFO", "root") as _:
                # Initialize local variables for execution.
                complete, started, failed, skipped = set(), set(), set(), set()
                events = set(roots)
                refs = {}
                # Start execution of root node functions.
                with _futures.ThreadPoolExecutor() as executor:
                    for event in events:
                        refs[event] = executor.submit(
                            _event_wrapper,
                            _callable=graph.nodes[event]['callable'],
                            _name=graph.nodes[event]['name'],
                            _args=graph.nodes[event]['args'],
                            _kwargs=graph.nodes[event]['kwargs']
                        )
                        started.add(event)
                    # Loop until all nodes in the network are executed.
                    while (len(complete) + len(failed) + len(skipped)) != len(graph):
                        for event in events:
                            if refs[event].done() is True:
                                if refs[event].exception(timeout=0) is not None:
                                    try:
                                        raise refs[event].exception(timeout=0)
                                    except:                                                                             # pylint: disable=bare-except
                                        _traceback.print_exc()
                                        print(f"Event {event} exited with exception, skipping dependent events...")     # pylint: disable=line-too-long
                                        failed.add(event)
                                        skipped = skipped.union(_nx.descendants(graph, event))
                                        started = started.union(skipped)
                                        events  = started.difference(complete.union(failed).union(skipped))             # pylint: disable=line-too-long
                                        continue
                                complete.add(event)
                                successors = [
                                    successor for successor in list(graph.successors(event)) if
                                    complete.issuperset(set(graph.predecessors(successor)))
                                ]
                                for successor in successors:
                                    _args = [
                                        a if not isinstance(a, _xcoms.Promise) else refs[a.key].result()
                                        for a in graph.nodes[successor]['args']
                                    ]
                                    _kwargs = {
                                        k: v if not isinstance(v, _xcoms.Promise) else refs[v.key].result()
                                        for k, v in graph.nodes[successor]['kwargs'].items()
                                    }
                                    refs[successor] = executor.submit(
                                        _event_wrapper,
                                        _callable=graph.nodes[successor]['callable'],
                                        _name=graph.nodes[successor]['name'],
                                        _args=_args,
                                        _kwargs=_kwargs
                                    )
                                    started.add(successor)
                        # Initialized nodes that are incomplete or not yet documented as complete.
                        events = started.difference(complete.union(failed).union(skipped))

    def validate_promise(self, promise: _xcoms.Promise) -> bool:
        """
        Returns True if a given Promise is valid, based on the DAG's current scope.

        Raises `webber.xcom.InvalidCallable` if Promise requests a callable that is out of scope.
        """
        if promise.key not in self.graph.nodes:
            err_msg = f"Requested callable {promise.key} is not defined in DAG's scope."
            raise _xcoms.InvalidCallable(err_msg)
        return True

    def add_node(self, node, *args, **kwargs) -> str:
        """
        Adds a callable with positional and keyword arguments to the DAG's underlying graph.

        On success, return unique identifier for the new node.
        """
        if not callable(node):
            err_msg = f"{node}: requested node is not a callable Python function."
            raise TypeError(err_msg)

        node_name = _edges.label_node(node)

        for arg in args:
            if isinstance(arg, _xcoms.Promise):
                self.validate_promise(arg)

        for val in kwargs.values():
            if isinstance(val, _xcoms.Promise):
                self.validate_promise(val)

        self.graph.add_node(
            node_for_adding=node_name,
            callable=node, args=args, kwargs=kwargs,
            name=node.__name__,
        )

        return node_name


    def add_edge(self, u_of_edge: _T.Union[str,_T.Callable], v_of_edge: _T.Union[str,_T.Callable]) -> _T.Tuple[str,str]: # pylint: disable=line-too-long,too-many-branches,too-many-statements
        """
        Adds an edge between nodes in the DAG's underlying graph,
        so long as the requested edge is unique and has not been added previously.

        On success, returns Tuple of the new edge's unique identifiers.
        """

        # Ensure both nodes are either callable or in string format.
        if not (isinstance(u_of_edge,str) or callable(u_of_edge)):
            err_msg = f"Outgoing node {u_of_edge} must be a string or a Python callable"
            raise TypeError(err_msg)
        if not (isinstance(v_of_edge,str) or callable(v_of_edge)):
            err_msg = f"Outgoing node {v_of_edge} must be a string or a Python callable"
            raise TypeError(err_msg)

        new_callable = None

        # Base Case 0: No nodes are present in the DAG:
        # Ensure that both nodes are callables, then add both to the graph and
        # assign the outgoing node as a root.
        if len(self.graph.nodes()) == 0:
            if not callable(u_of_edge):
                err_msg = f"Outgoing node {u_of_edge} is not defined in this DAG's scope."
                raise ValueError(err_msg)
            if not callable(v_of_edge):
                err_msg = f"Incoming node {v_of_edge} is not defined in this DAG's scope."
                raise ValueError(err_msg)
            outgoing_node = self.add_node(u_of_edge)
            incoming_node = self.add_node(v_of_edge)
            self.graph.add_edge(outgoing_node, incoming_node)
            return (outgoing_node, incoming_node)

        node_names, node_callables = zip(*self.graph.nodes(data='callable'))
        graph_edges = list(self.graph.edges(data=False))

        if callable(u_of_edge) and callable(v_of_edge):

            # Base Case 1: Both nodes are callable and are not present in the DAG:
            # Add both nodes to the DAG and assign the outgoing node as a root.
            if u_of_edge not in node_callables and v_of_edge not in node_callables:
                outgoing_node = self.add_node(u_of_edge)
                incoming_node = self.add_node(v_of_edge)
                return outgoing_node, incoming_node

            # Error Cases 0, 1: Either of the callables appear more than once in the DAG.
            if node_callables.count(u_of_edge) > 1:
                err_msg = f"Outgoing callable {u_of_edge.__name__} " \
                        + "exists more than once in this DAG. " \
                        + "Use the unique identifier of the required node or add a new node."
                raise ValueError(err_msg)
            if node_callables.count(v_of_edge) > 1:
                err_msg = f"Incoming callable {v_of_edge.__name__} " \
                        + "exists more than once in this DAG. " \
                        + "Use the unique identifier of the required node or add a new node."
                raise ValueError(err_msg)

            # Both callables exist only once in the DAG -- we can now use their unique identifiers.
            outgoing_node = node_names[ node_callables.index(u_of_edge) ]
            incoming_node = node_names[ node_callables.index(v_of_edge) ]

        else:

            # Error Cases 2, 3: Either of the requested IDs are not in the DAG's current scope.
            if isinstance(u_of_edge, str) and u_of_edge not in node_names:
                err_msg = f"Outgoing node {u_of_edge} not in DAG's current scope."
                raise ValueError(err_msg)
            if isinstance(v_of_edge, str) and v_of_edge not in node_names:
                err_msg = f"Outgoing node {v_of_edge} not in DAG's current scope."
                raise ValueError(err_msg)

            # Both nodes' unique identifiers are present in the DAG
            # and should be evaluated for a valid edge.
            if isinstance(u_of_edge, str) and isinstance(v_of_edge, str):
                outgoing_node = u_of_edge
                incoming_node = v_of_edge

            # Otherwise, one of the nodes is a callable, and the other is a valid unique identifier.
            else:
                if callable(u_of_edge):

                    # Error Case 4: The requested callable exists more than once in the DAG.
                    if node_callables.count(u_of_edge) > 1:
                        err_msg = f"Outgoing callable {u_of_edge.__name__} " \
                                + "exists more than once in this DAG. " \
                                + "Use the unique ID of the required node or add a new node."
                        raise ValueError(err_msg)

                    # If the callable exists only once in the DAG, use its unique identifier to
                    # evaluate the requested edge.
                    if node_callables.count(u_of_edge) == 1:
                        outgoing_node = node_names[ node_callables.index(u_of_edge) ]

                    # If the callable has never been added to the DAG, generate temporary unique ID
                    # and flag the callable for later addition if requested edge passes validation.
                    else:
                        new_callable  = u_of_edge
                        outgoing_node = _edges.label_node(u_of_edge)

                    incoming_node = v_of_edge

                else:

                    # Error Case 5: The requested callable exists more than once in the DAG.
                    if node_callables.count(v_of_edge) > 1:
                        err_msg = f"Outgoing callable {v_of_edge.__name__} " \
                                + "exists more than once in this DAG. " \
                                + "Use the unique identifier of the required node."
                        raise ValueError(err_msg)

                    # If the callable exists only once in the DAG, use its unique identifier to
                    # evaluate the requested edge.
                    if node_callables.count(v_of_edge) == 1:
                        incoming_node = node_names[ node_callables.index(v_of_edge) ]

                    # If the callable has never been added to the DAG, generate temporary unique ID
                    # and flag the callable for later addition if requested edge passes validation.
                    else:
                        new_callable  = v_of_edge
                        incoming_node = _edges.label_node(v_of_edge)

                    outgoing_node = u_of_edge

        # Error Case 6: Both callables exist only once in the DAG,
        # but an edge already exists between them.
        if (outgoing_node, incoming_node) in graph_edges:
            err_msg = f"Requested edge ({outgoing_node}, {incoming_node}) already has " \
                    + "a definition in this DAG."
            raise ValueError(err_msg)

        # Ensure that no cycles will be created by adding this edge to the DAG.
        test_edges: list = graph_edges + [(outgoing_node, incoming_node)]

        # Error Case 7: Both callables exist only once in the DAG,
        # but adding an edge between them creates circular dependencies.
        if not _nx.is_directed_acyclic_graph(_nx.DiGraph(test_edges)):
            err_msg = f"Requested edge ({outgoing_node}, {incoming_node}) " \
                    + "results in circular dependecies."
            raise ValueError(err_msg)

        # We can now add the edge to the DAG, since we are certain it will not result in
        # illegal dependencies/behavior.

        # First, we should account for potential new nodes:
        if new_callable == u_of_edge:
            outgoing_node = self.add_node(u_of_edge)

        elif new_callable == v_of_edge:
            incoming_node = self.add_node(v_of_edge)

        # Then we can add the new edge and re-evaluate the roots in our graph.
        self.graph.add_edge(outgoing_node, incoming_node)
        return (outgoing_node, incoming_node)

    def __init__(self, graph: _nx.DiGraph = None) -> None:

        if graph is None:
            self.graph = _nx.DiGraph()
            return
        
        _edges.validate_dag(graph)

        # Define framework specific logic as nested dictionaries.
        for node in graph.nodes.keys():
            graph.nodes[node]['callable'] = node
            graph.nodes[node]['name'] = node.__name__
            graph.nodes[node]['args'] = []
            graph.nodes[node]['kwargs'] = {}

        graph = _nx.relabel_nodes(graph, lambda node: _edges.label_node(node))
        self.graph = _nx.DiGraph(graph)

    def execute(self, return_ref=False):
        """
        Basic wrapper for execution of the DAG's underlying callables.
        """
        executor = self.DAGExecutor(self.graph, self.root)
        return executor if return_ref else None


    def visualize(self, vis_type: _T.Literal['gui', 'browser', 'plt'] = 'browser'):
        """
        Basic wrapper to visualize DAG using Vis.js library.
        """
        if vis_type == 'browser':
            _viz.visualize_browser(self.graph)

        elif vis_type == 'plt':
            _viz.visualize_plt(self.graph)

        elif vis_type == 'gui':
            # _visualize_gui(self.graph)
            raise NotImplementedError

        else:
            err_msg = "Unknown visualization type requested."
            raise NotImplementedError(err_msg)
