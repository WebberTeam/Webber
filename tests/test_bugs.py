"""
Regression tests for previously fixed critical bugs in Webber v0.2.

These tests ensure that bugs don't reappear in future versions.
"""
import pytest
import webber
from webber import Promise


class TestCallableValidation:
    """
    Prevent regression of inverted validation logic
    """

    def test_update_node_with_valid_callable(self, empty_dag):
        """Updating a node with a valid callable should succeed."""
        node1 = empty_dag.add_node(lambda: "original")

        # Update with new valid callable
        new_callable = lambda: "updated"
        empty_dag.update_nodes(node1, callable=new_callable)

        # Verify update succeeded
        assert empty_dag.get_node(node1)['callable'] == new_callable

    def test_update_node_with_invalid_callable(self, empty_dag):
        """Updating a node with non-callable should raise TypeError."""
        node1 = empty_dag.add_node(lambda: "test")

        # Attempt to update with non-callable
        with pytest.raises(TypeError, match="not assigned a callable Python function"):
            empty_dag.update_nodes(node1, callable="not a function")

    def test_update_node_with_integer_callable(self, empty_dag):
        """Updating a node with integer should raise TypeError."""
        node1 = empty_dag.add_node(lambda: "test")

        with pytest.raises(TypeError, match="not assigned a callable Python function"):
            empty_dag.update_nodes(node1, callable=42)


class TestPromiseResolution:
    """
    Prevent regression where promise resolution checks wrong variable
    """

    def test_promise_resolution_in_args(self, empty_dag):
        """Promise objects in positional args should resolve to string IDs."""
        node1 = empty_dag.add_node(lambda: "result")

        # Create Promise and add it as an arg
        promise = Promise(node1)
        node2 = empty_dag.add_node(lambda x: x, promise)

        # Check that Promise was resolved to string ID
        node2_data = empty_dag.get_node(node2)
        assert isinstance(node2_data['args'][0], Promise)
        assert isinstance(node2_data['args'][0].key, str)

    def test_multiple_promises_in_args(self, empty_dag):
        """Multiple Promise objects in args should all resolve."""
        node1 = empty_dag.add_node(lambda: "first")
        node2 = empty_dag.add_node(lambda: "second")
        node3 = empty_dag.add_node(lambda: "third")

        # Node with multiple Promise args
        node4 = empty_dag.add_node(
            lambda a, b, c: f"{a}-{b}-{c}",
            Promise(node1),
            Promise(node2),
            Promise(node3)
        )

        # All Promises should be resolved
        node4_data = empty_dag.get_node(node4)
        for arg in node4_data['args']:
            assert isinstance(arg, Promise)
            assert isinstance(arg.key, str)

    def test_mixed_promise_and_regular_args(self, empty_dag):
        """Args with both Promises and regular values should handle correctly."""
        node1 = empty_dag.add_node(lambda: 42)

        # Mix Promise and regular args
        node2 = empty_dag.add_node(
            lambda a, b, c: a + b + c,
            Promise(node1),  # Promise
            10,              # Regular value
            Promise(node1)   # Another Promise
        )

        node2_data = empty_dag.get_node(node2)
        assert isinstance(node2_data['args'][0], Promise)  # First is Promise
        assert node2_data['args'][1] == 10                 # Second is int
        assert isinstance(node2_data['args'][2], Promise)  # Third is Promise

    def test_promise_in_kwargs(self, empty_dag):
        """Promise objects in keyword arguments should resolve."""
        node1 = empty_dag.add_node(lambda: 100)

        node2 = empty_dag.add_node(lambda x=0: x * 2, x=Promise(node1))

        node2_data = empty_dag.get_node(node2)
        assert isinstance(node2_data['kwargs']['x'], Promise)
        assert isinstance(node2_data['kwargs']['x'].key, str)

    def test_single_arg_promise_not_confused_with_tuple(self, empty_dag):
        """
        Regression test: Ensure we check isinstance(arg, Promise)
        not isinstance(args, Promise) in the list comprehension.
        """
        node1 = empty_dag.add_node(lambda: "value")

        # Single Promise arg
        node2 = empty_dag.add_node(lambda x: x.upper(), Promise(node1))

        node2_data = empty_dag.get_node(node2)
        # Should have exactly 1 arg that is a Promise
        assert len(node2_data['args']) == 1
        assert isinstance(node2_data['args'][0], Promise)
