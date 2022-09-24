"""
Simple example of visualizing Webber graphs using built-in GUIs.
"""
import networkx as nx
import webber

a = lambda: print(1)
b = lambda: print(2)

if __name__ == "__main__":
    G = nx.DiGraph(
        [(a, b)]
    )
    dag = webber.DAG(G)
    dag.visualize()
