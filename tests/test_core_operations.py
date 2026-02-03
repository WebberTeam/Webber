"""
Core DAG Operations Tests - Priority P0

Tests for basic DAG operations: node addition, edge addition/removal,
node updates, queries, and filtering.
"""
import pytest
import webber
from webber import Promise


class TestNodeAddition:
    """Tests for DAG.add_node() functionality."""

    def test_add_lambda_node(self, empty_dag):
        """Adding a lambda function as a node should succeed."""
        node_id = empty_dag.add_node(lambda: "test")

        assert node_id in empty_dag.graph.nodes
        assert callable(empty_dag.get_node(node_id)['callable'])

    def test_add_function_node(self, empty_dag):
        """Adding a regular function as a node should succeed."""
        def my_func():
            return "test"

        node_id = empty_dag.add_node(my_func)

        assert node_id in empty_dag.graph.nodes
        node_data = empty_dag.get_node(node_id)
        assert node_data['callable'] == my_func
        assert node_data['name'] == 'my_func'

    def test_add_node_with_args_kwargs(self, empty_dag):
        """Adding node with positional and keyword arguments."""
        def my_func(a, b, c=10):
            return a + b + c

        node_id = empty_dag.add_node(my_func, 1, 2, c=3)

        node_data = empty_dag.get_node(node_id)
        assert node_data['args'] == (1, 2)
        assert node_data['kwargs'] == {'c': 3}

    def test_add_multiple_nodes(self, empty_dag):
        """Adding multiple nodes should create unique IDs."""
        node1 = empty_dag.add_node(lambda: "one")
        node2 = empty_dag.add_node(lambda: "two")
        node3 = empty_dag.add_node(lambda: "three")

        assert len(empty_dag.graph.nodes) == 3
        assert node1 != node2
        assert node2 != node3
        assert node1 != node3

    def test_add_non_callable_node(self, empty_dag):
        """Adding non-callable should raise TypeError."""
        with pytest.raises(TypeError, match="not a callable Python function"):
            empty_dag.add_node("not a function")

    def test_add_duplicate_callable(self, empty_dag):
        """Adding the same callable multiple times should create unique nodes."""
        def shared_func():
            return "shared"

        node1 = empty_dag.add_node(shared_func)
        node2 = empty_dag.add_node(shared_func)

        assert node1 != node2  # Should have unique IDs
        assert len(empty_dag.graph.nodes) == 2

    def test_add_node_with_only_kwargs(self, empty_dag):
        """Adding node with only keyword arguments."""
        node_id = empty_dag.add_node(lambda x=1, y=2: x + y, x=10, y=20)

        node_data = empty_dag.get_node(node_id)
        assert node_data['args'] == ()
        assert node_data['kwargs'] == {'x': 10, 'y': 20}


class TestEdgeAddition:
    """Tests for DAG.add_edge() functionality."""

    def test_add_edge_between_nodes(self, empty_dag):
        """Adding edge between two existing nodes."""
        node1 = empty_dag.add_node(lambda: "first")
        node2 = empty_dag.add_node(lambda: "second")

        edge = empty_dag.add_edge(node1, node2)

        assert edge == (node1, node2)
        assert (node1, node2) in empty_dag.graph.edges

    def test_add_edge_creates_nodes(self, empty_dag):
        """Adding edge with callable identifiers should create nodes if missing."""
        def func1():
            return "one"

        def func2():
            return "two"

        edge = empty_dag.add_edge(func1, func2)

        assert len(empty_dag.graph.nodes) == 2
        assert len(empty_dag.graph.edges) == 1

    def test_add_duplicate_edge(self, empty_dag):
        """Adding the same edge twice should raise ValueError."""
        node1 = empty_dag.add_node(lambda: "test")
        node2 = empty_dag.add_node(lambda: "test2")

        empty_dag.add_edge(node1, node2)

        with pytest.raises(ValueError, match="already has"):
            empty_dag.add_edge(node1, node2)

    def test_add_self_loop_edge(self, empty_dag):
        """Adding self-loop edge should raise error (creates cycle)."""
        node1 = empty_dag.add_node(lambda: "test")

        with pytest.raises(ValueError, match="circular dependencies"):
            empty_dag.add_edge(node1, node1)

    def test_add_edge_creating_cycle(self, empty_dag):
        """Adding edge that creates cycle should raise ValueError."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")
        node3 = empty_dag.add_node(lambda: "3")

        empty_dag.add_edge(node1, node2)
        empty_dag.add_edge(node2, node3)

        # This would create a cycle: 1 -> 2 -> 3 -> 1
        with pytest.raises(ValueError, match="circular dependencies"):
            empty_dag.add_edge(node3, node1)

    def test_add_multiple_edges_from_one_node(self, empty_dag):
        """One node should be able to have multiple outgoing edges."""
        root = empty_dag.add_node(lambda: "root")
        child1 = empty_dag.add_node(lambda: "child1")
        child2 = empty_dag.add_node(lambda: "child2")

        empty_dag.add_edge(root, child1)
        empty_dag.add_edge(root, child2)

        assert (root, child1) in empty_dag.graph.edges
        assert (root, child2) in empty_dag.graph.edges

    def test_add_multiple_edges_to_one_node(self, empty_dag):
        """One node should be able to have multiple incoming edges."""
        parent1 = empty_dag.add_node(lambda: "parent1")
        parent2 = empty_dag.add_node(lambda: "parent2")
        child = empty_dag.add_node(lambda: "child")

        empty_dag.add_edge(parent1, child)
        empty_dag.add_edge(parent2, child)

        assert (parent1, child) in empty_dag.graph.edges
        assert (parent2, child) in empty_dag.graph.edges


class TestEdgeRemoval:
    """Tests for DAG.remove_edge() functionality."""

    def test_remove_edge(self, empty_dag):
        """Removing an existing edge should succeed."""
        node1 = empty_dag.add_node(lambda: "test1")
        node2 = empty_dag.add_node(lambda: "test2")
        empty_dag.add_edge(node1, node2)

        removed = empty_dag.remove_edge(node1, node2)

        assert removed == (node1, node2)
        assert (node1, node2) not in empty_dag.graph.edges

    def test_remove_nonexistent_edge(self, empty_dag):
        """Removing non-existent edge should raise ValueError."""
        node1 = empty_dag.add_node(lambda: "test1")
        node2 = empty_dag.add_node(lambda: "test2")

        with pytest.raises(ValueError, match="does not exist"):
            empty_dag.remove_edge(node1, node2)

    def test_remove_edge_nodes_remain(self, empty_dag):
        """Removing edge should not remove the nodes."""
        node1 = empty_dag.add_node(lambda: "test1")
        node2 = empty_dag.add_node(lambda: "test2")
        empty_dag.add_edge(node1, node2)

        empty_dag.remove_edge(node1, node2)

        assert node1 in empty_dag.graph.nodes
        assert node2 in empty_dag.graph.nodes


class TestNodeUpdates:
    """Tests for DAG.update_nodes() functionality."""

    def test_update_node_args(self, empty_dag):
        """Updating node args should succeed."""
        node1 = empty_dag.add_node(lambda x: x, 10)

        empty_dag.update_nodes(node1, args=(20,))

        assert empty_dag.get_node(node1)['args'] == (20,)

    def test_update_node_kwargs(self, empty_dag):
        """Updating node kwargs should succeed."""
        node1 = empty_dag.add_node(lambda x=1: x, x=5)

        empty_dag.update_nodes(node1, kwargs={'x': 10})

        assert empty_dag.get_node(node1)['kwargs'] == {'x': 10}

    def test_update_multiple_nodes(self, empty_dag):
        """Updating multiple nodes simultaneously with args."""
        node1 = empty_dag.add_node(lambda x: x, 1)
        node2 = empty_dag.add_node(lambda x: x, 2)

        # Update both nodes to have same args
        empty_dag.update_nodes(node1, node2, args=(42,))

        assert empty_dag.get_node(node1)['args'] == (42,)
        assert empty_dag.get_node(node2)['args'] == (42,)

    def test_update_node_callable(self, empty_dag):
        """Updating node callable should work."""
        node1 = empty_dag.add_node(lambda: "original")

        new_callable = lambda: "updated"
        empty_dag.update_nodes(node1, callable=new_callable)

        assert empty_dag.get_node(node1)['callable'] == new_callable

    def test_update_node_with_kwargs(self, empty_dag):
        """Updating node kwargs for multiple nodes."""
        node1 = empty_dag.add_node(lambda x=1: x)
        node2 = empty_dag.add_node(lambda y=2: y)

        # Update both nodes with same kwargs
        empty_dag.update_nodes(node1, node2, kwargs={'value': 99})

        assert empty_dag.get_node(node1)['kwargs'] == {'value': 99}
        assert empty_dag.get_node(node2)['kwargs'] == {'value': 99}


class TestNodeQueries:
    """Tests for node retrieval and querying."""

    def test_get_node_by_id(self, empty_dag):
        """Retrieving node by string ID."""
        node_id = empty_dag.add_node(lambda: "test")

        node_data = empty_dag.get_node(node_id)

        assert node_data['id'] == node_id
        assert callable(node_data['callable'])

    def test_get_node_by_callable(self, empty_dag):
        """Retrieving node by callable reference."""
        def my_func():
            return "test"

        empty_dag.add_node(my_func)

        node_data = empty_dag.get_node(my_func)

        assert node_data['callable'] == my_func

    def test_get_multiple_nodes(self, empty_dag):
        """Retrieving multiple nodes at once."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")

        nodes = empty_dag.get_nodes(node1, node2)

        assert len(nodes) == 2
        assert nodes[0]['id'] == node1
        assert nodes[1]['id'] == node2

    def test_get_all_nodes(self, empty_dag):
        """Retrieving all nodes when no identifiers provided."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")
        node3 = empty_dag.add_node(lambda: "3")

        nodes = empty_dag.get_nodes()

        assert len(nodes) == 3
        node_ids = [n['id'] for n in nodes]
        assert node1 in node_ids
        assert node2 in node_ids
        assert node3 in node_ids

    def test_filter_nodes(self, empty_dag):
        """Filtering nodes using lambda function."""
        def special_func():
            return "special"

        def regular_func():
            return "regular"

        node1 = empty_dag.add_node(special_func)
        node2 = empty_dag.add_node(regular_func)

        # Filter by name
        filtered = empty_dag.filter_nodes(lambda n: n['name'] == 'special_func', data=True)

        assert len(filtered) == 1
        assert filtered[0]['id'] == node1

    def test_filter_nodes_returns_ids(self, empty_dag):
        """Filtering nodes with data=False should return IDs only."""
        def special_func():
            return "special"

        def regular_func():
            return "regular"

        node1 = empty_dag.add_node(special_func)
        node2 = empty_dag.add_node(regular_func)

        # Filter by name, return IDs only
        filtered = empty_dag.filter_nodes(lambda n: n['name'] == 'special_func', data=False)

        assert len(filtered) == 1
        assert filtered[0] == node1
        assert isinstance(filtered[0], str)

    def test_get_nonexistent_node(self, empty_dag):
        """Attempting to get non-existent node should raise error."""
        with pytest.raises((ValueError, KeyError)):
            empty_dag.get_node("nonexistent_id")


class TestEdgeQueries:
    """Tests for edge retrieval and querying."""

    def test_get_edge_with_data(self, empty_dag):
        """Retrieving edge with metadata."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")
        empty_dag.add_edge(node1, node2)

        edge_data = empty_dag.get_edge(node1, node2, data=True)

        assert edge_data['parent'] == node1
        assert edge_data['child'] == node2
        assert 'Condition' in edge_data

    def test_get_edge_without_data(self, empty_dag):
        """Retrieving edge as tuple without metadata."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")
        empty_dag.add_edge(node1, node2)

        edge_tuple = empty_dag.get_edge(node1, node2, data=False)

        assert edge_tuple == (node1, node2)

    def test_get_all_edges(self, empty_dag):
        """Retrieving all edges."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")
        node3 = empty_dag.add_node(lambda: "3")
        empty_dag.add_edge(node1, node2)
        empty_dag.add_edge(node2, node3)

        edges = empty_dag.get_edges()

        assert len(edges) == 2

    def test_filter_edges(self, diamond_dag):
        """Filtering edges using lambda function."""
        dag, root, left, right, merge = diamond_dag

        # Filter edges going to merge node
        to_merge = dag.filter_edges(lambda e: e.child == merge, data=True)

        assert len(to_merge) == 2  # left->merge and right->merge

    def test_get_nonexistent_edge(self, empty_dag):
        """Attempting to get non-existent edge should raise error."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")

        with pytest.raises(ValueError, match="No match found"):
            empty_dag.get_edge(node1, node2, data=True)


class TestNodeLabeling:
    """Tests for node labeling/renaming."""

    def test_relabel_node(self, empty_dag):
        """Relabeling node should update name."""
        node = empty_dag.add_node(lambda: "test")

        new_label = empty_dag.relabel_node(node, "custom_name")

        assert new_label == "custom_name"
        assert empty_dag.get_node(node)['name'] == "custom_name"

    def test_relabel_with_empty_string(self, empty_dag):
        """Relabeling with empty string should raise ValueError."""
        node = empty_dag.add_node(lambda: "test")

        with pytest.raises(ValueError, match="one or more characters"):
            empty_dag.relabel_node(node, "")

    def test_relabel_with_non_string(self, empty_dag):
        """Relabeling with non-string should raise ValueError."""
        node = empty_dag.add_node(lambda: "test")

        with pytest.raises(ValueError, match="Python string"):
            empty_dag.relabel_node(node, 123)


class TestDAGProperties:
    """Tests for DAG properties and metadata."""

    def test_root_property(self, empty_dag):
        """Root property should return nodes with no dependencies."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")
        node3 = empty_dag.add_node(lambda: "3")
        empty_dag.add_edge(node1, node2)

        roots = empty_dag.root

        assert node1 in roots  # Has no incoming edges
        assert node3 in roots  # Has no incoming edges
        assert node2 not in roots  # Has incoming edge from node1

    def test_nodes_property(self, empty_dag):
        """Nodes property should provide access to graph nodes."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")

        nodes = empty_dag.nodes

        assert node1 in nodes
        assert node2 in nodes

    def test_node_id_validation(self, empty_dag):
        """node_id() should validate and return string ID."""
        def my_func():
            return "test"

        node_id = empty_dag.add_node(my_func)

        # Should work with string ID
        assert empty_dag.node_id(node_id) == node_id

        # Should work with callable
        assert empty_dag.node_id(my_func) == node_id

    def test_node_id_with_invalid_identifier(self, empty_dag):
        """node_id() with invalid identifier should raise TypeError."""
        # Add a node so DAG is not empty
        empty_dag.add_node(lambda: "test")

        with pytest.raises(TypeError, match="must be a string or a Python callable"):
            empty_dag.node_id(123)
