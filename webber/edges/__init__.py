"""
Helper class for edge and DAG validation logic.
"""
import typing as _T
import uuid as _uuid
import networkx as _nx
import enum as _enum

__all__ = ["valid_node", "valid_nodes", "valid_dag", "validate_nodes", "label_node"]

class Condition(_enum.IntEnum):
    """Represents edge condition for a node execution, based on outcome(s) of predecessor(s)."""
    Success = 0
    Failure = 1
    AnyCase = 3

class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

class edgedict(dotdict):
    """Dictionary subclass for representing DAG edges with dot notation access.

    Provides convenient access to edge properties: parent, child, id, and Condition.
    """
    super(dotdict)
    def __init__(self, *E: _T.Any, **kwargs: _T.Any) -> None:
        """Initialize edge dictionary with parent/child nodes and optional attributes."""
        super().__init__({'parent': E[0], 'child': E[1], 'id': E[:2]})
        self.update(kwargs)

def continue_on_failure(edge: _T.Dict[str, _T.Any]) -> bool:
    """Check edge condition for whether to continue on parent node's failure."""
    return edge['Condition'] in (Condition.Failure, Condition.AnyCase)

def continue_on_success(edge: _T.Dict[str, _T.Any]) -> bool:
    """Check edge condition for whether to continue on parent node's success."""
    return edge['Condition'] in (Condition.Success, Condition.AnyCase)

def label_node(node: _T.Callable) -> str:
    """Generates unique identifiers for Python callables in a DAG using UUIDs."""
    return f"{node.__name__}__{_uuid.uuid4()}"

def get_root(graph: _nx.DiGraph) -> _T.List[str]:
    """Given a network graph, return list of all nodes without incoming edges or dependencies.
    Uses O(1) in_degree() instead of O(k) predecessors list creation."""
    return [node for node in graph.nodes if graph.in_degree(node) == 0]

# TODO: Refactor logic for DAG and node validation.

def valid_node(node: str | _T.Callable) -> bool:
    """Check whether given identifier represents a valid node (string or callable)."""
    return (isinstance(node,str) or callable(node))

def valid_nodes(u_of_edge: str | _T.Callable, v_of_edge: str | _T.Callable) -> bool:
    """Check whether parent and child nodes represent valid nodes (string or callable)."""
    return valid_node(u_of_edge) and valid_node(v_of_edge)

def validate_nodes(u_of_edge: str | _T.Callable, v_of_edge: str | _T.Callable) -> bool:
    """
    Given parent and child identifiers, validate that both represent valid nodes.
    Otherwise raise exceptions.
    """
    if not valid_node(u_of_edge):
        err_msg = f"Outgoing node {u_of_edge} must be a string or a Python callable"
        raise TypeError(err_msg)

    if not valid_node(v_of_edge):
        err_msg = f"Incoming node {v_of_edge} must be a string or a Python callable"
        raise TypeError(err_msg)

    return True

def valid_dag(graph: _nx.Graph) -> bool:
    """
    Given a network graph, return whether network is a valid DAG and that all node-keys are Python callables.
    Meant for internal use, DAG initialization.
    """
    return (
        isinstance(graph, _nx.Graph) and
        _nx.is_directed_acyclic_graph(graph) and
        all(callable(node) for node in graph.nodes)
    )


def validate_dag(graph: _nx.DiGraph) -> None:
    """
    Given a network graph, validate whether graph is a valid Webber DAG. Otherwise, raise exceptions.
    Meant for internal use, DAG initialization.
    """
    if not graph.is_directed():
        err_msg = f"Directed graph must be defined as type {_nx.DiGraph.__name__}"
        raise TypeError(err_msg)

    if any(not callable(node) for node in graph.nodes):
        err_msg = "All registered nodes must be callable Python functions."
        raise TypeError(err_msg)

    if not _nx.is_directed_acyclic_graph(graph):
        err_msg = "Directed acyclic graph must be properly defined --" \
                + "no cycles and one or more root nodes."
        raise ValueError(err_msg)
