"""
Simple example of visualizing Webber graphs using built-in GUIs.
"""
import networkx as nx
import webber

def foo_a():
    """A simple foo!"""
    print(1)

def foo_b():
    """Another simple foo!"""
    print(2)

if __name__ == "__main__":
    G = nx.DiGraph([
        (foo_a, foo_b)
    ])
    dag = webber.DAG(G)
    dag.visualize()
