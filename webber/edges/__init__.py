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

def label_node(node: _T.Callable) -> str:
    return f"{node.__name__}__{_uuid.uuid1()}"

