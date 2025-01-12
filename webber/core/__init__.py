"""
Base module for abstract multiprocessing system - a directed acyclic graph.
"""
import sys as _sys
import typing as _T
import uuid as _uuid
import types as _types
import logging as _logging
import traceback as _traceback
import contextlib as _contextlib
import collections.abc as _abc
import concurrent.futures as _futures
import networkx as _nx
import webber.edges as _edges
import webber.xcoms as _xcoms
import webber.queue as _queue

from webber.edges import Condition, dotdict, edgedict

import queue as _q
import itertools as _it

__all__ = ["DAG", "Condition"]

def _iscallable(function: any):
    return callable(function)

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
        if not _iscallable(node):
            err_msg = f"{node}: requested node is not a callable Python function."
            raise TypeError(err_msg)

        node_name = _edges.label_node(node)

        args = tuple([
            arg if not isinstance(args, _xcoms.Promise) else self.resolve_promise(arg)
            for arg in args
        ])

        for k, val in kwargs.items():
            if isinstance(val, _xcoms.Promise):
                kwargs[k] = self.resolve_promise(val)

        self.graph.add_node(
            node_for_adding=node_name,
            callable=node, args=args, kwargs=kwargs,
            name=node.__name__,
            id=node_name
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
        if not (isinstance(u_of_edge,str) or _iscallable(u_of_edge)):
            err_msg = f"Outgoing node {u_of_edge} must be a string or a Python callable"
            raise TypeError(err_msg)
        if not (isinstance(v_of_edge,str) or _iscallable(v_of_edge)):
            err_msg = f"Outgoing node {v_of_edge} must be a string or a Python callable"
            raise TypeError(err_msg)

        new_callable = None

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

        node_names, node_callables = zip(*self.graph.nodes(data='callable'))
        graph_edges = list(self.graph.edges(data=False))

        if _iscallable(u_of_edge) and _iscallable(v_of_edge):

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
                if _iscallable(u_of_edge):

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

    def update_edges(self, *E, continue_on: Condition, filter: _types.LambdaType = None, data = None):
        """
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

        if filter != None:
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

        std_update = (continue_on == None)

        if std_update:
            for edge_id, e in zip(edge_ids, E):
                if data != None:
                    self._update_edge(data, id = edge_id)
                else:
                    self._update_edge(e, id = edge_id)
        
        else:
            if continue_on != None:
                if not isinstance(continue_on, Condition):
                    err_msg = f"Condition assignment must use webber.edges.Condition"
                    raise TypeError(err_msg)
                for e in edge_ids:
                    self.graph.edges[e]['Condition'] = continue_on

    def _update_edge(self, edgedict: dict, id: tuple[str, str] = None, force: bool = False):
        if id != None:
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

            if edgedict.get('parent') or edgedict.get('child'):
                e2 = self.get_edge(edgedict.get('parent'), edgedict.get('child'), data = False)
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

    def update_nodes(self, *N, filter: _types.LambdaType = None, data = None, callable = None, args = None, kwargs = None):
        """
        """
        if len(N) == 0 and filter == None:
            raise ValueError("Either an array of node IDs or node data (N) or a filter must be passed to this function.")

        elif isinstance(N, dict) or isinstance(N, str):
            N = [N]

        elif len(N) == 1 and isinstance(N[0], _abc.Iterable):
            if isinstance(N[0][0], dict):
                N = N[0]
            elif isinstance(N[0][0], str):
                # BUG: A list of all single character IDs will fail to be updated. Please try another call method (i.e.: nested iterator).
                if sum(list(map(lambda n: len(n), N[0]))) != len(N[0]): 
                    N = N[0]


        if filter != None:
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
                if data != None:
                    self._update_node(data, id = node_id)
                else:
                    self._update_node(n, id = node_id)
        
        else:
            if callable != None:
                if not _iscallable(callable):
                    err_msg = f"Requested node is not assigned a callable Python function."
                    raise TypeError(err_msg)
                for node_id in node_ids:
                    self.graph.nodes[node_id]['callable'] = callable
                    self.graph.nodes[node_id]['name'] = callable.__name__
            
            if args != None:
                if not (isinstance(args, _abc.Iterable) and not isinstance(args, str)):
                    err_msg = f"Requested node is not assigned a tuple of pos args."
                    raise TypeError(err_msg)
                args = tuple(args)
                for node_id in node_ids:
                    self.graph.nodes[node_id]['args'] = args
            
            if kwargs != None:
                if not isinstance(kwargs, dict):
                    err_msg = f"Requested node is not assigned a dictionary of kw args."
                    raise TypeError(err_msg)
                for node_id in node_ids:
                    self.graph.nodes[node_id]['kwargs'] = kwargs

    def get_edges(self, *N, data: bool = True) -> _T.Union[list[edgedict], list[tuple]]:
        """
        Retrieval function for DAG edge data, based on tuple identifiers.
        Use filter_edges for more flexible controls (e.g.: filter_edges(in=['node_1', 'node_2']))
        """
        if len(N) == 0:
            if data == True:
                return list(map(edgedict, self.graph.edges.data()))
            return list(self.graph.edges.data(data=False))
            
        # elif len(N) == 1:
        #     if isinstance(N[0], _abc.Iterable) and not isinstance(N[0], tuple):
        #         N = N[0]

        if len(N) != len(set(N)) or False in map(lambda n: isinstance(n, _abc.Iterable) and len(n) == 2, N):
            err_msg = 'All requested edges must be unique tuples of size 2.'
            raise ValueError(err_msg)
    
        edge_data = [self.get_edge(o, i, data=data) for (o, i) in N]
        return edge_data
    
    def get_edge(self, outgoing_node: _T.Union[str, callable], incoming_node: _T.Union[str, callable], data: bool = True) -> _T.Union[edgedict, tuple]:
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
        return edgedict(*id, **edge_data)

    def get_node(self, n: _T.Union[str, callable]) -> dotdict:
        """
        Given a unique identifier, returns a dictionary of node metadata
        for a single node in the DAG's scope.
        """
        node_id = self.node_id(n)
        return dotdict(self.graph.nodes[node_id])

    def get_nodes(self, *N) -> list[dotdict]:
        """
        Flexible function to retrieve DAG node data, based on node identifiers 
        (e.g.: string IDs or unique callables).
        """
        if len(N) == 0:
            node_data = list(self.graph.nodes.values())
            # for i in range(len(node_data)):
            #     _id = node_data[i][0]
            #     node_data[i] = node_data[i][1]
            #     node_data[i]['id'] = _id
            return [dotdict(d) for d in node_data]

        elif len(N) == 1:
            if isinstance(N[0], _abc.Iterable) and not isinstance(N[0], str):
                N = N[0]
            else:
                node_id = self.node_id(N[0])
                node_data = dotdict(self.graph.nodes[node_id])
                # node_data['id'] = node_id
                return node_data
            
        if not len(N) == len(set(N)):
            err_msg = 'All requested nodes must be unique identifiers.'
            raise ValueError(err_msg)
        
        node_ids  = [self.node_id(n) for n in N]
        node_data = [dotdict(self.graph.nodes[n]) for n in node_ids]
        # for i, _id in enumerate(node_ids):
        #     node_data[i]['id'] = _id
        return node_data

    def filter_nodes(self, filter: _types.LambdaType, data: bool = False):
        """
        Given a lambda function, filter nodes in a DAG's scope based on its attributes.
        Current limitation: Filters must use node identifier strings when referencing nodes.
        Use get_nodes for more flexible controls.
        """
        if not data:
            return [node['id'] for node in self.graph.nodes.values() if filter(dotdict(node))]
        return [dotdict(node) for node in self.graph.nodes.values() if filter(dotdict(node))]
    
    def filter_edges(self, filter = _types.LambdaType, data: bool = False) -> list[edgedict]:
        """
        Given a lambda function, filter edges in a DAG's scope based on its attributes.
        Current limitation: Filters must use node identifier strings when referencing nodes.
        Use get_edges for more flexible controls.
        """
        if not data:
            return [e[:2] for e in list(self.graph.edges.data()) if filter(edgedict(e))]
        return [edgedict(e) for e in list(self.graph.edges.data()) if filter(edgedict(e))]

    def retry_node(self, identifier: _T.Union[str,_T.Callable], count: int):
        """
        Given a node identifier, set number of automatic retries in case of failure.
        Re-attempts will begin as soon as possible.
        """
        if not isinstance(count, int) or not count >= 0:
            raise ValueError("Retry count must be a non-negative integer.")
        node = self.node_id(identifier)
        self.graph.nodes[node]['retry'] = count

    def skip_node(self, identifier: _T.Union[str,_T.Callable], skip: bool = True, as_failure = False):
        """
        Given a node identifier, set DAG to skip node execution as a success (stdout print) or a failure (exception error).
        Allows conditional control and testing over DAG's order of operations.
        """
        if not isinstance(skip, bool):
            raise ValueError("Skip argument must be a boolean value.")
        node = self.node_id(identifier)
        self.graph.nodes[node]['skip'] = (skip, as_failure)
      
    def critical_path(self, nodes):
        """
        Given a set of nodes, returns a subset of the DAG containing
        only the node(s) and its parents, or upstream dependencies.
        """
        if isinstance(nodes, _abc.Iterable) and not isinstance(nodes, str):
            node_ids = {self.node_id(n) for n in nodes}
        else:
            node_ids = {self.node_id(nodes)}
        return self._subgraph(node_ids)

    def execute(self, return_ref=False, print_exc=False):
        """
        Basic wrapper for execution of the DAG's underlying callables.
        """
        executor = self.DAGExecutor(self.graph, self.root, print_exc)
        return executor if return_ref else None

    def visualize(self, type: _T.Literal['gui', 'browser', 'plt'] = None):
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
    def root(self) -> list[str]:
        """
        Return list of nodes with no dependencies.
        Root nodes will occur first in DAG's order of operations.
        """
        return list(filter(
            lambda node: len(list(self.graph.predecessors(node))) < 1,
            self.graph.nodes.keys()
        ))

    @property
    def nodes(self):
        return self.graph.nodes

    def node_id(self, identifier: _T.Union[str,_T.Callable]) -> str:
        """
        Validate whether identifier given is a valid node within the DAG's scope.
        Primarily for internal use, but useful for retrieving string identifiers
        for a unique callable in a DAG.
        """
        node_names, node_callables = zip(*self.graph.nodes(data='callable'))

        if isinstance(identifier, str):
            if identifier not in node_names:
                err_msg = f"Node {identifier} is not defined in this DAG's scope."
                raise ValueError(err_msg)
            node = identifier
        elif _iscallable(identifier):
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
    
    def _update_node(self, nodedict: dict, id: str = None, force: bool = False):
        """
        """
        if id != None:
            try:
                if nodedict.get('id') != None and id != nodedict['id']:
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
                if _iscallable(nodedict['callable']):
                    err_msg = f"Requested node is not assigned a callable Python function."
                    raise TypeError(err_msg)
                if not nodedict.get('name'):
                    nodedict['name'] = nodedict['callable'].__name__

            if nodedict.get('name') and (not isinstance(nodedict['name'], str) or len(nodedict['name']) == 0): 
                err_msg = f"Requested node name must be a non-null Python string, will default to callable when not set."
                raise TypeError(err_msg)

            if nodedict.get('args'):
                if not (isinstance(nodedict['args'], _abc.Iterable) or isinstance(nodedict['args'], str)):
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

    def resolve_promise(self, promise: _xcoms.Promise) -> _xcoms.Promise:
        """
        Returns a Promise with a unique string identifier, if a given Promise is valid, based on the DAG's current scope.
        Raises `webber.xcom.InvalidCallable` if Promise requests a callable that is out of scope.
        """
        try:
            key = self.node_id(promise.key)
        except Exception as e:
            raise _xcoms.InvalidCallable(e)
        return _xcoms.Promise(key)

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
        for n in graph.nodes:
            graph.nodes[n]['id'] = n
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
        
    def add_edge(self, u_of_edge, v_of_edge, continue_on = Condition.Success):
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

    def execute(self, *promises, return_ref=False, print_exc=False):
        """
        Basic wrapper for execution of the DAG's underlying callables.
        """

        queues = {}
        processes = {}
        join = set()
        
        promises: dict = { k: v for k, v in _it.pairwise(promises) } if len(promises) > 0 else {}

        with _OutputLogger(str(_uuid.uuid4()), "INFO", "root") as _:
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
                    queues[id] = _q.LifoQueue()
                    if len(list(self.graph.successors(id))) == 0:
                        endproc = id
                    node.update({
                        'callable': _queue._worker,
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
