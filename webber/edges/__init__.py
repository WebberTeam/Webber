import typing as _T
import uuid as _uuid
import networkx as _nx

__all__ = ["valid_node", "valid_nodes", "valid_dag", "validate_nodes", "label_node"]

def valid_node(node: _T.Union[str, _T.Callable]) -> bool:
    return (isinstance(node,str) or callable(node))

def valid_nodes(u_of_edge: _T.Union[str, _T.Callable], v_of_edge: _T.Union[str, _T.Callable]) -> bool:
    return valid_node(u_of_edge) and valid_node(v_of_edge)

def valid_dag(graph: _nx.Graph) -> bool:
    return (
        isinstance(graph, _nx.Graph) and
        _nx.is_directed_acyclic_graph(graph) and
        not set(map(callable, list(graph.nodes.keys()))).issuperset({False})
    )

def validate_nodes(u_of_edge: _T.Union[str, _T.Callable], v_of_edge: _T.Union[str, _T.Callable]) -> True:
    
    if not valid_node(u_of_edge):
        err_msg = f"Outgoing node {u_of_edge} must be a string or a Python callable"
        raise TypeError(err_msg)
    
    if not valid_node(v_of_edge):
        err_msg = f"Incoming node {v_of_edge} must be a string or a Python callable"
        raise TypeError(err_msg)
    
    return True

def validate_dag(graph: _nx.DiGraph):
    if not graph.is_directed():
        err_msg = f"Directed graph must be defined as type {_nx.DiGraph.__name__}"
        raise TypeError(err_msg)

    if set(map(callable, list(graph.nodes.keys()))).issuperset({False}):
        err_msg = "All registered nodes must be callable Python functions."
        raise TypeError(err_msg)

    if not _nx.is_directed_acyclic_graph(graph):
        err_msg = "Directed acyclic graph must be properly defined --" \
                + "no cycles and one or more root nodes."
        raise ValueError(err_msg)

def get_root(graph: _nx.DiGraph) -> list:
    return list(filter(
        lambda node: len(list(graph.predecessors(node))) < 1,
        graph.nodes.keys()
    ))

def label_node(node: _T.Callable) -> str:
    return f"{node.__name__}__{_uuid.uuid4()}"
