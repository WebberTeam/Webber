"""
Edge Cases & Error Handling Tests - Priority P1

Tests for empty DAGs, disconnected nodes, invalid inputs, and large DAGs.
"""
import pytest
import webber
from webber import Condition, Promise


class TestEmptyDAG:
    """Tests for empty DAG handling."""

    def test_execute_empty_dag(self, empty_dag):
        """Executing DAG with no nodes should skip execution."""
        empty_dag.execute()  # Should not raise error

    def test_query_empty_dag(self, empty_dag):
        """Querying empty DAG should handle gracefully."""
        nodes = empty_dag.filter_nodes(lambda n: True)
        assert len(nodes) == 0

    def test_empty_dag_root(self, empty_dag):
        """Empty DAG should have no root nodes."""
        assert len(empty_dag.root) == 0

    def test_empty_dag_get_all_nodes(self, empty_dag):
        """Getting all nodes from empty DAG should return empty list."""
        nodes = empty_dag.get_nodes()
        assert len(nodes) == 0

    def test_empty_dag_get_all_edges(self, empty_dag):
        """Getting all edges from empty DAG should return empty list."""
        edges = empty_dag.get_edges()
        assert len(edges) == 0


class TestDisconnectedNodes:
    """Tests for DAGs with disconnected/independent nodes."""

    def test_disconnected_nodes_execute(self, empty_dag):
        """DAG with disconnected nodes should execute all independently."""
        results = []
        node1 = empty_dag.add_node(lambda: results.append("1"))
        node2 = empty_dag.add_node(lambda: results.append("2"))
        node3 = empty_dag.add_node(lambda: results.append("3"))

        # No edges - all independent
        empty_dag.execute()

        assert len(results) == 3
        assert set(results) == {"1", "2", "3"}

    def test_disconnected_with_failure(self, empty_dag):
        """Failure in one disconnected node shouldn't affect others."""
        results = []
        node1 = empty_dag.add_node(lambda: results.append("1"))
        node2 = empty_dag.add_node(lambda: 1/0)  # Will fail
        node3 = empty_dag.add_node(lambda: results.append("3"))

        empty_dag.execute()

        # node1 and node3 should still execute
        assert "1" in results
        assert "3" in results

    def test_partially_connected_dag(self, empty_dag):
        """DAG with some connected and some disconnected nodes."""
        results = []

        # Connected chain
        chain1 = empty_dag.add_node(lambda: results.append("chain1a"))
        chain2 = empty_dag.add_node(lambda: results.append("chain1b"))
        empty_dag.add_edge(chain1, chain2)

        # Separate disconnected nodes
        solo1 = empty_dag.add_node(lambda: results.append("solo1"))
        solo2 = empty_dag.add_node(lambda: results.append("solo2"))

        empty_dag.execute()

        assert len(results) == 4
        assert set(results) == {"chain1a", "chain1b", "solo1", "solo2"}


class TestInvalidInputs:
    """Tests for invalid inputs and error handling."""

    def test_none_as_node_identifier(self, empty_dag):
        """None should not be accepted as node identifier."""
        empty_dag.add_node(lambda: "test")  # Add a node so DAG is not empty

        with pytest.raises(TypeError):
            empty_dag.get_node(None)

    def test_invalid_condition_string(self, empty_dag):
        """Invalid Condition value should raise error."""
        node1 = empty_dag.add_node(lambda: "test")
        node2 = empty_dag.add_node(lambda: "test2")

        with pytest.raises((ValueError, TypeError)):
            empty_dag.add_edge(node1, node2, continue_on="invalid")

    def test_invalid_condition_none(self, empty_dag):
        """None as Condition should raise error."""
        node1 = empty_dag.add_node(lambda: "test")
        node2 = empty_dag.add_node(lambda: "test2")

        with pytest.raises((ValueError, TypeError)):
            empty_dag.add_edge(node1, node2, continue_on=None)

    def test_add_edge_nonexistent_nodes(self, empty_dag):
        """Adding edge with non-existent node IDs should raise error."""
        with pytest.raises((ValueError, KeyError)):
            empty_dag.add_edge("nonexistent1", "nonexistent2")

    def test_remove_nonexistent_node(self, empty_dag):
        """Removing non-existent edge should raise error."""
        node1 = empty_dag.add_node(lambda: "test")
        node2 = empty_dag.add_node(lambda: "test2")

        with pytest.raises(ValueError):
            empty_dag.remove_edge(node1, node2)  # Edge doesn't exist

    def test_get_nonexistent_node(self, empty_dag):
        """Getting non-existent node should raise error."""
        with pytest.raises((ValueError, KeyError)):
            empty_dag.get_node("nonexistent_id")

    def test_promise_to_nonexistent_node(self, empty_dag):
        """Promise to non-existent node should raise error."""
        promise = Promise("nonexistent_id")

        with pytest.raises(webber.xcoms.InvalidCallable):
            empty_dag.resolve_promise(promise)

    def test_update_nonexistent_node(self, empty_dag):
        """Updating non-existent node should raise error."""
        with pytest.raises((ValueError, KeyError)):
            empty_dag.update_nodes("nonexistent_id", args=(42,))

    def test_invalid_retry_count(self, empty_dag):
        """Invalid retry count should raise error."""
        node1 = empty_dag.add_node(lambda: "test")

        with pytest.raises(ValueError):
            empty_dag.retry_node(node1, count=-5)

    def test_invalid_skip_value(self, empty_dag):
        """Invalid skip value should raise error."""
        node1 = empty_dag.add_node(lambda: "test")

        with pytest.raises(ValueError):
            empty_dag.skip_node(node1, skip="not_boolean")


class TestLargeDAGs:
    """Tests for large DAG structures."""

    def test_wide_dag(self, empty_dag):
        """DAG with many parallel nodes should execute correctly."""
        root = empty_dag.add_node(lambda: "root")
        children = [empty_dag.add_node(lambda i=i: i) for i in range(100)]

        for child in children:
            empty_dag.add_edge(root, child)

        empty_dag.execute()  # Should complete without error

    def test_deep_dag(self, empty_dag):
        """DAG with long chain of dependencies should execute correctly."""
        results = []
        nodes = [empty_dag.add_node(lambda i=i: results.append(i) or i) for i in range(50)]

        for i in range(len(nodes) - 1):
            empty_dag.add_edge(nodes[i], nodes[i+1])

        empty_dag.execute()  # Should complete without error
        assert len(results) == 50

    def test_many_edges(self, empty_dag):
        """DAG with many edges should handle correctly."""
        # Create a fully connected DAG (every node to every other node where possible)
        nodes = [empty_dag.add_node(lambda i=i: i) for i in range(10)]

        edge_count = 0
        # Add edges in topological order to avoid cycles
        for i in range(len(nodes) - 1):
            for j in range(i + 1, len(nodes)):
                empty_dag.add_edge(nodes[i], nodes[j])
                edge_count += 1

        assert len(empty_dag.get_edges()) == edge_count
        empty_dag.execute()

    def test_large_number_of_nodes(self, empty_dag):
        """DAG should handle thousands of nodes."""
        # Create 1000 disconnected nodes
        nodes = [empty_dag.add_node(lambda i=i: i) for i in range(1000)]

        assert len(empty_dag.graph.nodes) == 1000
        empty_dag.execute()  # Should complete

    def test_diamond_pattern_repeated(self, empty_dag):
        """Complex DAG with repeated diamond patterns."""
        results = []

        # Create 10 diamond patterns
        for iteration in range(10):
            root = empty_dag.add_node(lambda it=iteration: results.append(f"root{it}"))
            left = empty_dag.add_node(lambda it=iteration: results.append(f"left{it}"))
            right = empty_dag.add_node(lambda it=iteration: results.append(f"right{it}"))
            merge = empty_dag.add_node(lambda it=iteration: results.append(f"merge{it}"))

            empty_dag.add_edge(root, left)
            empty_dag.add_edge(root, right)
            empty_dag.add_edge(left, merge)
            empty_dag.add_edge(right, merge)

        empty_dag.execute()
        assert len(results) == 40  # 4 nodes per pattern × 10 patterns


class TestCycleDetection:
    """Tests for cycle detection in DAG."""

    def test_simple_cycle(self, empty_dag):
        """Simple 2-node cycle should be detected."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")

        empty_dag.add_edge(node1, node2)

        with pytest.raises(ValueError, match="circular dependencies"):
            empty_dag.add_edge(node2, node1)

    def test_three_node_cycle(self, empty_dag):
        """Three-node cycle should be detected."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")
        node3 = empty_dag.add_node(lambda: "3")

        empty_dag.add_edge(node1, node2)
        empty_dag.add_edge(node2, node3)

        with pytest.raises(ValueError, match="circular dependencies"):
            empty_dag.add_edge(node3, node1)

    def test_self_loop(self, empty_dag):
        """Self-loop should be detected as cycle."""
        node1 = empty_dag.add_node(lambda: "test")

        with pytest.raises(ValueError, match="circular dependencies"):
            empty_dag.add_edge(node1, node1)

    def test_complex_cycle(self, empty_dag):
        """Complex cycle through multiple paths should be detected."""
        #     1
        #    / \
        #   2   3
        #    \ /
        #     4
        # Trying to add 4 -> 1 creates cycle
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")
        node3 = empty_dag.add_node(lambda: "3")
        node4 = empty_dag.add_node(lambda: "4")

        empty_dag.add_edge(node1, node2)
        empty_dag.add_edge(node1, node3)
        empty_dag.add_edge(node2, node4)
        empty_dag.add_edge(node3, node4)

        with pytest.raises(ValueError, match="circular dependencies"):
            empty_dag.add_edge(node4, node1)


class TestCriticalPath:
    """Tests for critical path extraction."""

    def test_critical_path_single_node(self, empty_dag):
        """Critical path of single node should return just that node."""
        node1 = empty_dag.add_node(lambda: "test")

        critical = empty_dag.critical_path(node1)

        assert len(critical.graph.nodes) == 1
        assert node1 in critical.graph.nodes

    def test_critical_path_with_dependencies(self, empty_dag):
        """Critical path should include all upstream dependencies."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")
        node3 = empty_dag.add_node(lambda: "3")
        node4 = empty_dag.add_node(lambda: "4")

        empty_dag.add_edge(node1, node2)
        empty_dag.add_edge(node2, node3)
        empty_dag.add_edge(node1, node4)

        # Critical path to node3 should include node1, node2, node3
        critical = empty_dag.critical_path(node3)

        assert node1 in critical.graph.nodes
        assert node2 in critical.graph.nodes
        assert node3 in critical.graph.nodes
        assert node4 not in critical.graph.nodes  # Not on path to node3

    def test_critical_path_multiple_nodes(self, empty_dag):
        """Critical path with multiple target nodes."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")
        node3 = empty_dag.add_node(lambda: "3")
        node4 = empty_dag.add_node(lambda: "4")
        node5 = empty_dag.add_node(lambda: "5")

        empty_dag.add_edge(node1, node2)
        empty_dag.add_edge(node2, node3)
        empty_dag.add_edge(node1, node4)
        empty_dag.add_edge(node4, node5)

        # Critical path to node3 and node5
        critical = empty_dag.critical_path([node3, node5])

        # Should include all nodes since both paths start from node1
        assert len(critical.graph.nodes) == 5

    def test_critical_path_execution(self, empty_dag):
        """Critical path subgraph should be executable."""
        results = []
        node1 = empty_dag.add_node(lambda: results.append("1"))
        node2 = empty_dag.add_node(lambda: results.append("2"))
        node3 = empty_dag.add_node(lambda: results.append("3"))
        node4 = empty_dag.add_node(lambda: results.append("4"))

        empty_dag.add_edge(node1, node2)
        empty_dag.add_edge(node2, node3)
        empty_dag.add_edge(node1, node4)

        critical = empty_dag.critical_path(node3)
        critical.execute()

        # Should execute node1, node2, node3 but not node4
        assert "1" in results
        assert "2" in results
        assert "3" in results
        assert "4" not in results
