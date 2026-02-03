"""
Pytest configuration and shared fixtures for Webber test suite.
"""
import pytest
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import webber
from webber import DAG, Promise, Condition, QueueDAG




@pytest.fixture
def empty_dag():
    """Provides a fresh empty DAG for each test."""
    return webber.DAG()


@pytest.fixture
def simple_dag():
    """Provides a DAG with two connected nodes."""
    dag = webber.DAG()
    node1 = dag.add_node(lambda: "first")
    node2 = dag.add_node(lambda: "second")
    dag.add_edge(node1, node2)
    return dag, node1, node2


@pytest.fixture
def dag_with_promises():
    """Provides a DAG with Promise-based dependencies."""
    dag = webber.DAG()
    node1 = dag.add_node(lambda: 42)
    node2 = dag.add_node(lambda x: x * 2, Promise(node1))
    dag.add_edge(node1, node2)
    return dag, node1, node2


@pytest.fixture
def diamond_dag():
    """
    Provides a DAG with diamond structure:
         root
        /    \\
      left  right
        \\    /
        merge
    """
    dag = webber.DAG()
    root = dag.add_node(lambda: "root")
    left = dag.add_node(lambda: "left")
    right = dag.add_node(lambda: "right")
    merge = dag.add_node(lambda: "merge")

    dag.add_edge(root, left)
    dag.add_edge(root, right)
    dag.add_edge(left, merge)
    dag.add_edge(right, merge)

    return dag, root, left, right, merge


@pytest.fixture
def empty_queuedag():
    """Provides a fresh empty QueueDAG for each test."""
    return webber.QueueDAG()
