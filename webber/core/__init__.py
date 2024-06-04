"""
Base module for abstract multiprocessing system - a directed acyclic graph.
"""
import sys as _sys
import typing as _T
import uuid as _uuid
import logging as _logging
import traceback as _traceback
import contextlib as _contextlib
import collections.abc as _abc
import concurrent.futures as _futures
import networkx as _nx
import webber.edges as _edges
import webber.xcoms as _xcoms
import webber.viz as _viz

from webber.edges import Condition

__all__ = ["DAG", "Condition"]

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
        pass

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
                self._validate_promise(arg)

        for val in kwargs.values():
            if isinstance(val, _xcoms.Promise):
                self._validate_promise(val)

        self.graph.add_node(
            node_for_adding=node_name,
            callable=node, args=args, kwargs=kwargs,
            name=node.__name__,
        )

        return node_name


    def add_edge(
            self, 
            u_of_edge: _T.Union[str,_T.Callable], v_of_edge: _T.Union[str,_T.Callable],
            continue_on: Condition = Condition.Success
        ) -> _T.Tuple[str,str]:
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
            self.graph.add_edge(outgoing_node, incoming_node, Condition = continue_on)
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
        self.graph.add_edge(outgoing_node, incoming_node, Condition = continue_on)
        return (outgoing_node, incoming_node)

    def retry_node(self, identifier: _T.Union[str,_T.Callable], count: int):
        """
        Given a node identifier, set number of automatic retries in case of failure.
        Re-attempts will begin as soon as possible.
        """
        if not isinstance(count, int) and not count > 0:
            raise ValueError("Retry count must be a positive integer.")
        node = self._node_id(identifier)
        self.graph.nodes[node]['retry'] = count

    def skip_node(self, identifier: _T.Union[str,_T.Callable], skip: bool = True, as_failure = False):
        """
        Given a node identifier, set DAG to skip node execution as a success (stdout print) or a failure (exception error).
        Allows conditional control and testing over DAG's order of operations.
        """
        if not isinstance(skip, bool):
            raise ValueError("Skip argument must be a boolean value.")
        node = self._node_id(identifier)
        self.graph.nodes[node]['skip'] = (skip, as_failure)

    @property
    def root(self) -> list[str]:
        """
        Return list of nodes with no dependencies.
        Root nodes will occur first in DAG's order of operations.
        """
        return list(filter(
            lambda node: len(list(self.graph.predecessors(node))) < 1,
            self.graph.nodes.keys()
        ))
       
    def critical_path(self, nodes):
        """
        Given a set of nodes, returns a subset of the DAG containing
        only the node(s) and its parents, or upstream dependencies.
        """
        if isinstance(nodes, _abc.Iterable) and not isinstance(nodes, str):
            node_ids = {self._node_id(n) for n in nodes}
        else:
            node_ids = {self._node_id(nodes)}
        return self._subgraph(node_ids)

    def execute(self, return_ref=False, print_exc=False):
        """
        Basic wrapper for execution of the DAG's underlying callables.
        """
        executor = self.DAGExecutor(self.graph, self.root, print_exc)
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
    
    def _node_id(self, identifier: _T.Union[str,_T.Callable]) -> str:
        """
        Meant for internal use only -- validate whether identifier given is a
        valid node within the DAG's scope.
        """
        node_names, node_callables = zip(*self.graph.nodes(data='callable'))

        if isinstance(identifier, str):
            if identifier not in node_names:
                err_msg = f"Node {identifier} is not defined in this DAG's scope."
                raise ValueError(err_msg)
            node = identifier
        elif callable(identifier):
            match node_callables.count(identifier):
                case 0:
                    err_msg = f"Callable {identifier} is not defined in this DAG's scope."
                    raise ValueError(err_msg)
                case 1:
                    node = node_names[ node_callables.index(identifier) ]
                case _:
                    err_msg = f"Callable {identifier.__name__} " \
                            + "exists more than once in this DAG. " \
                            + "Use the unique identifier of the required node."
                    raise ValueError(err_msg)
        else:            
            err_msg = f"Node {identifier} must be a string or a Python callable"
            raise TypeError(err_msg)
        return node
    
    def _subgraph(self, node_ids: set[str]):
        """
        Internal only. Given a set of nodes, returns a subset of the DAG containing
        only the node(s) and upstream dependencies.
        """
        parent_nodes = set()
        for node in node_ids:
            parent_nodes = parent_nodes.union(set(self.graph.predecessors(node)))
        while parent_nodes:
            node_ids = node_ids.union(parent_nodes)
            parent_nodes, child_nodes = set(), parent_nodes
            for node in child_nodes:
                parent_nodes = parent_nodes.union(set(self.graph.predecessors(node)))
        subgraph = self.graph.subgraph(node_ids)
        return DAG(subgraph, __force=True)

    def _validate_promise(self, promise: _xcoms.Promise) -> bool:
        """
        Returns True if a given Promise is valid, based on the DAG's current scope.
        Raises `webber.xcom.InvalidCallable` if Promise requests a callable that is out of scope.
        """
        if promise.key not in self.graph.nodes:
            err_msg = f"Requested callable {promise.key} is not defined in DAG's scope."
            raise _xcoms.InvalidCallable(err_msg)
        return True

    def __init__(self, graph: _nx.DiGraph = None, **kwargs) -> None:

        if graph is None:
            self.graph = _nx.DiGraph()
            return
        
        # Meant for internal use only, creating DAGs from subgraphs.
        if kwargs.get('__force') == True:
            self.graph = graph
            return
        
        _edges.validate_dag(graph)

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
        self.graph = _nx.DiGraph(graph)

    class DAGExecutor:
        """
        Base class used to execute DAG in embarrassingly parallel.
        """
        def __init__(self, graph: _nx.DiGraph, roots: list, print_exc: bool = False) -> None:
            with _OutputLogger(str(_uuid.uuid4()), "INFO", "root") as _:
                # Initialize local variables for execution.
                complete, started, failed, skipped = set(), set(), set(), set()
                events = set(roots)
                refs: dict[str, _futures.Future] = {}

                def raise_exc(message):
                    raise ValueError(message)

                def run_conditions_met(n):
                    for p in graph.predecessors(n):
                        match graph.edges.get((p, n))['Condition']:
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

                skip  = graph.nodes.data("skip", default=(False, False))
                retry = {n: [c+1, {}] for n,c in graph.nodes.data("retry", default=0)}

                # Start execution of root node functions.
                with _futures.ThreadPoolExecutor() as executor:

                    def Submit(event, callable, name, args, kwargs):
                        if skip[event][0]: # NOTE: Internally, DAGExecutor tracks these skipped events as successes or failures.
                            retry[event][0] = 0
                            skip_callable = raise_exc if skip[event][1] else print
                            return executor.submit(
                                _event_wrapper,
                                _callable=skip_callable,
                                _name=graph.nodes[event]['name'],
                                _args=[f"Event {event} skipped..."],
                                _kwargs={}
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
                                _kwargs=kwargs
                            )

                    for event in events:
                        refs[event] = Submit(
                            event,
                            graph.nodes[event]['callable'],
                            graph.nodes[event]['name'],
                            graph.nodes[event]['args'],
                            graph.nodes[event]['kwargs']
                        )
                        started.add(event)
                    # Loop until all nodes in the network are executed.
                    while (len(complete) + len(failed) + len(skipped)) != len(graph):
                        for event in events:
                            if refs[event].done() is True:
                                if refs[event].exception(timeout=0) is not None:
                                    try:
                                        raise refs[event].exception(timeout=0)
                                    except:
                                        if print_exc:
                                            _traceback.print_exc()
                                        
                                        if retry[event][0] > 0:
                                            print(f"Event {event} exited with exception, retrying...")
                                            refs[event] = Submit(
                                                event,
                                                callable=retry[event][1]['callable'],
                                                name=retry[event][1]['name'],
                                                args=retry[event][1]['args'],
                                                kwargs=retry[event][1]['kwargs']
                                            )
                                            continue                                            

                                        print(f"Event {event} exited with exception...")
                                        failed.add(event)
                                        skipping = [
                                            e[1] for e in set(graph.out_edges(event)) 
                                            if not _edges.continue_on_failure(graph.edges.get(e))
                                        ]
                                else:
                                    complete.add(event)
                                    skipping = [
                                        e[1] for e in set(graph.out_edges(event)) 
                                        if not _edges.continue_on_success(graph.edges.get(e))
                                    ]
                                skipped  = skipped.union(skipping)
                                for n in skipping:
                                    skipped  = skipped.union(_nx.descendants(graph, n))
                                carryon  = set(graph.successors(event)).difference(skipped)
                                starting = [
                                    successor for successor in carryon if
                                    run_conditions_met(successor)
                                ]
                                for successor in starting:
                                    _args = [
                                        a if not isinstance(a, _xcoms.Promise) else refs[a.key].result()
                                        for a in graph.nodes[successor]['args']
                                    ]
                                    _kwargs = {
                                        k: v if not isinstance(v, _xcoms.Promise) else refs[v.key].result()
                                        for k, v in graph.nodes[successor]['kwargs'].items()
                                    }
                                    refs[successor] = Submit(
                                        successor,
                                        graph.nodes[successor]['callable'],
                                        graph.nodes[successor]['name'],
                                        _args,
                                        _kwargs
                                    )
                                    started.add(successor)
                        # Initialized nodes that are incomplete or not yet documented as complete.
                        events = started.difference(complete.union(failed).union(skipped))
