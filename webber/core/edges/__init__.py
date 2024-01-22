import uuid
import networkx
from typing import Callable, Union

def valid_node(node: Union[str, Callable]) -> bool:
    return (isinstance(node,str) or callable(node))

def valid_nodes(u_of_edge: Union[str, Callable], v_of_edge: Union[str, Callable]) -> bool:
    return valid_node(u_of_edge) and valid_node(v_of_edge)

def valid_dag(graph: networkx.Graph) -> bool:
    return (
        isinstance(graph, networkx.Graph) and
        networkx.is_directed_acyclic_graph(graph) and
        not set(map(callable, list(graph.nodes.keys()))).issuperset({False})
    )

def validate_nodes(u_of_edge: Union[str, Callable], v_of_edge: Union[str, Callable]) -> True:
    
    if not valid_node(u_of_edge):
        err_msg = f"Outgoing node {u_of_edge} must be a string or a Python callable"
        raise TypeError(err_msg)
    
    if not valid_node(v_of_edge):
        err_msg = f"Incoming node {v_of_edge} must be a string or a Python callable"
        raise TypeError(err_msg)
    
    return True

def label_node(node: Callable) -> str:
    return f"{node.__name__}__{uuid.uuid1()}"

