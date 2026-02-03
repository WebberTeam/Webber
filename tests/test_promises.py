"""
Promise System Tests - Priority P0

Tests for Promise creation, resolution, and usage in node arguments.
"""
import pytest
import webber
from webber import Promise
from webber.xcoms import InvalidCallable


class TestPromiseCreation:
    """Tests for creating and resolving Promise objects."""

    def test_promise_with_callable_key(self, empty_dag):
        """Creating Promise with callable as key."""
        def my_func():
            return 42

        empty_dag.add_node(my_func)

        promise = Promise(my_func)
        resolved = empty_dag.resolve_promise(promise)

        assert isinstance(resolved, Promise)
        assert isinstance(resolved.key, str)  # Should resolve to string ID

    def test_promise_with_string_key(self, empty_dag):
        """Creating Promise with string ID as key."""
        node_id = empty_dag.add_node(lambda: "test")

        promise = Promise(node_id)
        resolved = empty_dag.resolve_promise(promise)

        assert isinstance(resolved, Promise)
        assert resolved.key == node_id

    def test_promise_with_invalid_key(self, empty_dag):
        """Promise referencing non-existent node should raise InvalidCallable."""
        promise = Promise("nonexistent_node_id")

        with pytest.raises(InvalidCallable):
            empty_dag.resolve_promise(promise)

    def test_promise_out_of_scope(self, empty_dag):
        """Promise referencing callable not in DAG should raise InvalidCallable."""
        def not_in_dag():
            return "missing"

        empty_dag.add_node(lambda: "exists")

        promise = Promise(not_in_dag)

        with pytest.raises(InvalidCallable):
            empty_dag.resolve_promise(promise)

    def test_promise_with_integer_key_raises_error(self):
        """Creating Promise with invalid key type should raise TypeError."""
        with pytest.raises(TypeError, match="Keys must be string IDs or callables"):
            Promise(123)

    def test_promise_with_none_key_raises_error(self):
        """Creating Promise with None should raise TypeError."""
        with pytest.raises(TypeError, match="Keys must be string IDs or callables"):
            Promise(None)

    def test_promise_with_dict_key_raises_error(self):
        """Creating Promise with dict should raise TypeError."""
        with pytest.raises(TypeError, match="Keys must be string IDs or callables"):
            Promise({'key': 'value'})


class TestPromiseInArguments:
    """Tests for Promise objects in node arguments."""

    def test_promise_in_args(self, empty_dag):
        """Promise in positional arguments should be stored correctly."""
        node1 = empty_dag.add_node(lambda: 100)

        node2 = empty_dag.add_node(lambda x: x * 2, Promise(node1))

        node2_data = empty_dag.get_node(node2)
        assert isinstance(node2_data['args'][0], Promise)
        assert isinstance(node2_data['args'][0].key, str)

    def test_promise_in_kwargs(self, empty_dag):
        """Promise objects in keyword arguments should be stored correctly."""
        node1 = empty_dag.add_node(lambda: 100)

        node2 = empty_dag.add_node(lambda x=0: x * 2, x=Promise(node1))

        node2_data = empty_dag.get_node(node2)
        assert isinstance(node2_data['kwargs']['x'], Promise)
        assert isinstance(node2_data['kwargs']['x'].key, str)

    def test_multiple_promises_in_args(self, empty_dag):
        """Multiple Promises in args should all be handled."""
        node1 = empty_dag.add_node(lambda: 1)
        node2 = empty_dag.add_node(lambda: 2)
        node3 = empty_dag.add_node(lambda: 3)

        node4 = empty_dag.add_node(
            lambda a, b, c: a + b + c,
            Promise(node1),
            Promise(node2),
            Promise(node3)
        )

        node4_data = empty_dag.get_node(node4)
        assert len(node4_data['args']) == 3
        for arg in node4_data['args']:
            assert isinstance(arg, Promise)

    def test_mixed_promise_and_literals(self, empty_dag):
        """Mixing Promises with literal values in args."""
        node1 = empty_dag.add_node(lambda: 10)

        node2 = empty_dag.add_node(
            lambda a, b, c: f"{a}-{b}-{c}",
            Promise(node1),  # Promise
            "literal",       # String literal
            42               # Integer literal
        )

        node2_data = empty_dag.get_node(node2)
        assert isinstance(node2_data['args'][0], Promise)
        assert node2_data['args'][1] == "literal"
        assert node2_data['args'][2] == 42

    def test_promise_in_nested_structures(self, empty_dag):
        """Promises within lists/dicts should be preserved."""
        node1 = empty_dag.add_node(lambda: 42)

        # Promise inside a list
        node2 = empty_dag.add_node(
            lambda data: data,
            [Promise(node1), "other"]
        )

        node2_data = empty_dag.get_node(node2)
        assert len(node2_data['args']) == 1
        assert isinstance(node2_data['args'][0], list)


class TestPromiseExecution:
    """Tests for Promise resolution during DAG execution."""

    def test_promise_execution_simple(self, empty_dag):
        """During execution, Promises should resolve to actual return values."""
        results = []

        node1 = empty_dag.add_node(lambda: 42)
        node2 = empty_dag.add_node(lambda x: results.append(x) or x * 2, Promise(node1))

        empty_dag.add_edge(node1, node2)
        empty_dag.execute()

        # Verify node2 received the value from node1
        assert results == [42]

    def test_promise_execution_with_kwargs(self, empty_dag):
        """Promises in kwargs should resolve during execution."""
        results = []

        node1 = empty_dag.add_node(lambda: 100)
        node2 = empty_dag.add_node(
            lambda multiplier: results.append(multiplier) or multiplier * 3,
            multiplier=Promise(node1)
        )

        empty_dag.add_edge(node1, node2)
        empty_dag.execute()

        assert results == [100]

    def test_promise_chain(self, empty_dag):
        """Chaining multiple nodes with Promises."""
        node1 = empty_dag.add_node(lambda: 5)
        node2 = empty_dag.add_node(lambda x: x * 2, Promise(node1))
        node3 = empty_dag.add_node(lambda x: x + 10, Promise(node2))

        empty_dag.add_edge(node1, node2)
        empty_dag.add_edge(node2, node3)

        empty_dag.execute()
        # If execution completes without error, test passes

    def test_multiple_promises_execution(self, empty_dag):
        """Node with multiple Promise dependencies."""
        results = []

        node1 = empty_dag.add_node(lambda: 10)
        node2 = empty_dag.add_node(lambda: 20)
        node3 = empty_dag.add_node(
            lambda a, b: results.append((a, b)) or a + b,
            Promise(node1),
            Promise(node2)
        )

        empty_dag.add_edge(node1, node3)
        empty_dag.add_edge(node2, node3)
        empty_dag.execute()

        assert results == [(10, 20)]

    def test_promise_with_complex_return_value(self, empty_dag):
        """Promises should handle complex return values (lists, dicts)."""
        results = []

        node1 = empty_dag.add_node(lambda: {"key": "value", "count": 42})
        node2 = empty_dag.add_node(
            lambda data: results.append(data['count']),
            Promise(node1)
        )

        empty_dag.add_edge(node1, node2)
        empty_dag.execute()

        assert results == [42]


class TestPromiseEdgeCases:
    """Edge cases and error handling for Promises."""

    def test_promise_to_same_node(self, empty_dag):
        """Self-loop edges should be caught as circular dependencies."""
        # Create a simple node
        node1 = empty_dag.add_node(lambda x: x, 42)

        # Adding self-loop edge should fail (circular dependency)
        with pytest.raises(ValueError, match="circular dependencies"):
            empty_dag.add_edge(node1, node1)

    def test_promise_without_edge(self, empty_dag):
        """Promise without corresponding edge should still work if nodes execute."""
        node1 = empty_dag.add_node(lambda: 42)
        node2 = empty_dag.add_node(lambda x: x * 2, Promise(node1))

        # Note: Without edge, execution order is not guaranteed
        # This tests that having a Promise doesn't break execution
        empty_dag.execute()

    def test_promise_with_failed_parent(self, empty_dag):
        """Promise to failed node should not execute child (with Success condition)."""
        results = []

        node1 = empty_dag.add_node(lambda: 1 / 0)  # Will fail
        node2 = empty_dag.add_node(
            lambda x: results.append(x),
            Promise(node1)
        )

        empty_dag.add_edge(node1, node2)  # Default: Success condition
        empty_dag.execute()

        # node2 should not execute because node1 failed
        assert len(results) == 0

    def test_promise_resolution_with_duplicate_callable(self, empty_dag):
        """Promise resolution when callable added multiple times."""
        def shared_func():
            return "shared"

        node1 = empty_dag.add_node(shared_func)
        node2 = empty_dag.add_node(shared_func)

        # Promise to first instance
        promise1 = Promise(node1)
        resolved1 = empty_dag.resolve_promise(promise1)
        assert resolved1.key == node1

        # Promise to second instance
        promise2 = Promise(node2)
        resolved2 = empty_dag.resolve_promise(promise2)
        assert resolved2.key == node2

        # Promise to callable should raise error (ambiguous)
        with pytest.raises(InvalidCallable):
            promise_ambiguous = Promise(shared_func)
            empty_dag.resolve_promise(promise_ambiguous)

    def test_update_node_with_promise_args(self, empty_dag):
        """Updating node args to include Promises."""
        node1 = empty_dag.add_node(lambda: 42)
        node2 = empty_dag.add_node(lambda: "original")

        # Update node2 to use Promise
        empty_dag.update_nodes(node2, args=(Promise(node1),))

        node2_data = empty_dag.get_node(node2)
        assert isinstance(node2_data['args'][0], Promise)
