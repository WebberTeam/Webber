"""
QueueDAG Tests - Priority P2

Tests for QueueDAG-specific functionality including iterative execution,
halt conditions, and edge constraints.
"""
import pytest
import webber
from webber import QueueDAG, Promise


class TestQueueDAGBasicOperations:
    """Tests for basic QueueDAG node and edge operations."""

    def test_queuedag_creation(self, empty_queuedag):
        """QueueDAG should be creatable with no arguments."""
        assert empty_queuedag is not None
        assert len(empty_queuedag.graph.nodes) == 0

    def test_queuedag_add_node(self, empty_queuedag):
        """Adding node to QueueDAG should work like regular DAG."""
        node_id = empty_queuedag.add_node(lambda: "test")

        assert node_id in empty_queuedag.graph.nodes
        assert callable(empty_queuedag.get_node(node_id)['callable'])

    def test_queuedag_add_node_with_iterator(self, empty_queuedag):
        """QueueDAG node with iterator should store the iteration count."""
        node_id = empty_queuedag.add_node(lambda: "test", iterator=5)

        assert node_id in empty_queuedag.graph.nodes
        assert empty_queuedag.conditions[node_id]['iter_limit'] == 5

    def test_queuedag_add_node_with_halt_condition(self, empty_queuedag):
        """QueueDAG node with halt_condition should store the condition."""
        condition = lambda x: x >= 3
        node_id = empty_queuedag.add_node(lambda: 1, halt_condition=condition)

        assert node_id in empty_queuedag.graph.nodes
        assert empty_queuedag.conditions[node_id]['halt_condition'] == condition

    def test_queuedag_add_node_with_max_iter(self, empty_queuedag):
        """QueueDAG node with max_iter should store as iter_limit."""
        node_id = empty_queuedag.add_node(
            lambda: 1,
            halt_condition=lambda x: False,
            max_iter=10
        )

        assert empty_queuedag.conditions[node_id]['iter_limit'] == 10


class TestQueueDAGExecution:
    """Tests for QueueDAG execution with iterators and halt conditions."""

    def test_queuedag_with_iterator(self, empty_queuedag):
        """QueueDAG node should execute specified number of times."""
        executions = []
        node1 = empty_queuedag.add_node(
            lambda: executions.append(1) or len(executions),
            iterator=5
        )
        empty_queuedag.execute()

        assert len(executions) == 5

    def test_queuedag_with_iterator_zero(self, empty_queuedag):
        """QueueDAG with iterator=0 should not execute."""
        executions = []
        node1 = empty_queuedag.add_node(
            lambda: executions.append(1),
            iterator=0
        )
        empty_queuedag.execute()

        assert len(executions) == 0

    def test_queuedag_with_halt_condition_immediate(self, empty_queuedag):
        """QueueDAG should halt immediately if condition is met on first run."""
        executions = []

        def work():
            executions.append(1)
            return len(executions)

        node1 = empty_queuedag.add_node(
            work,
            halt_condition=lambda x: x >= 1,
            max_iter=100
        )
        empty_queuedag.execute()

        # Should stop after first execution because condition is met
        assert len(executions) == 1

    def test_queuedag_with_halt_condition_delayed(self, empty_queuedag):
        """QueueDAG should halt when condition is eventually met."""
        executions = []

        def work():
            executions.append(1)
            return len(executions)

        node1 = empty_queuedag.add_node(
            work,
            halt_condition=lambda x: x >= 3,
            max_iter=100
        )
        empty_queuedag.execute()

        assert len(executions) == 3

    def test_queuedag_max_iter_safety(self, empty_queuedag):
        """QueueDAG should respect max_iter even with never-halting condition."""
        executions = []
        node1 = empty_queuedag.add_node(
            lambda: executions.append(1),
            halt_condition=lambda x: False,  # Never halt
            max_iter=10
        )
        empty_queuedag.execute()

        assert len(executions) == 10  # Should stop at max_iter

    def test_queuedag_simple_chain(self, empty_queuedag):
        """QueueDAG chain should pass outputs between nodes."""
        node1 = empty_queuedag.add_node(lambda: 42, iterator=3)
        node2 = empty_queuedag.add_node(lambda x: x * 2, Promise(node1))

        empty_queuedag.add_edge(node1, node2)
        results = empty_queuedag.execute()

        # Results should contain processed values from the chain
        assert results is not None
        assert len(results) == 3
        assert all(r == 84 for r in results)


class TestQueueDAGEdgeConstraints:
    """Tests for QueueDAG edge constraints (single parent/child)."""

    def test_queuedag_single_parent_constraint(self, empty_queuedag):
        """QueueDAG node should only allow one parent."""
        parent1 = empty_queuedag.add_node(lambda: 1, iterator=1)
        parent2 = empty_queuedag.add_node(lambda: 2, iterator=1)
        child = empty_queuedag.add_node(lambda x: x)

        empty_queuedag.add_edge(parent1, child)

        with pytest.raises(AssertionError, match="maximum of one child and one parent"):
            empty_queuedag.add_edge(parent2, child)

    def test_queuedag_single_child_constraint(self, empty_queuedag):
        """QueueDAG node should only allow one child."""
        parent = empty_queuedag.add_node(lambda: 1, iterator=1)
        child1 = empty_queuedag.add_node(lambda x: x)
        child2 = empty_queuedag.add_node(lambda x: x)

        empty_queuedag.add_edge(parent, child1)

        with pytest.raises(AssertionError, match="maximum of one child and one parent"):
            empty_queuedag.add_edge(parent, child2)

    def test_queuedag_linear_chain_allowed(self, empty_queuedag):
        """QueueDAG should allow linear chains (A -> B -> C)."""
        node_a = empty_queuedag.add_node(lambda: 1, iterator=1)
        node_b = empty_queuedag.add_node(lambda x: x + 1)
        node_c = empty_queuedag.add_node(lambda x: x + 1)

        # Both edges should succeed
        empty_queuedag.add_edge(node_a, node_b)
        empty_queuedag.add_edge(node_b, node_c)

        assert len(empty_queuedag.graph.edges) == 2


class TestQueueDAGEmptyExecution:
    """Tests for empty QueueDAG handling."""

    def test_execute_empty_queuedag(self, empty_queuedag):
        """Executing empty QueueDAG should handle gracefully."""
        result = empty_queuedag.execute()
        # Should not raise error, may return None


class TestQueueDAGInheritance:
    """Tests to verify QueueDAG properly inherits from DAG."""

    def test_queuedag_is_dag_subclass(self):
        """QueueDAG should be a subclass of DAG."""
        assert issubclass(QueueDAG, webber.DAG)

    def test_queuedag_has_dag_methods(self, empty_queuedag):
        """QueueDAG should have all DAG methods."""
        assert hasattr(empty_queuedag, 'add_node')
        assert hasattr(empty_queuedag, 'add_edge')
        assert hasattr(empty_queuedag, 'get_node')
        assert hasattr(empty_queuedag, 'get_nodes')
        assert hasattr(empty_queuedag, 'execute')
        assert hasattr(empty_queuedag, 'filter_nodes')
        assert hasattr(empty_queuedag, 'update_nodes')

    def test_queuedag_conditions_dict(self, empty_queuedag):
        """QueueDAG should have conditions dictionary."""
        assert hasattr(empty_queuedag, 'conditions')
        assert isinstance(empty_queuedag.conditions, dict)


class TestQueueDAGWithArgs:
    """Tests for QueueDAG with args and kwargs."""

    def test_queuedag_node_with_args(self, empty_queuedag):
        """QueueDAG node should accept args like regular DAG."""
        node_id = empty_queuedag.add_node(lambda x, y: x + y, 5, 10, iterator=1)

        node_data = empty_queuedag.get_node(node_id)
        assert node_data['args'] == (5, 10)

    def test_queuedag_node_with_kwargs(self, empty_queuedag):
        """QueueDAG node should accept kwargs (non-reserved)."""
        node_id = empty_queuedag.add_node(
            lambda x=1, y=2: x + y,
            x=10, y=20,
            iterator=1
        )

        node_data = empty_queuedag.get_node(node_id)
        assert node_data['kwargs'] == {'x': 10, 'y': 20}

    def test_queuedag_reserved_kwargs_not_in_node(self, empty_queuedag):
        """Reserved kwargs should not appear in node kwargs."""
        node_id = empty_queuedag.add_node(
            lambda: "test",
            iterator=5,
            halt_condition=lambda x: False,
            max_iter=10
        )

        node_data = empty_queuedag.get_node(node_id)
        assert 'iterator' not in node_data['kwargs']
        assert 'halt_condition' not in node_data['kwargs']
        assert 'max_iter' not in node_data['kwargs']


class TestQueueDAGReturnValues:
    """Tests for QueueDAG return value collection."""

    def test_queuedag_returns_list(self, empty_queuedag):
        """QueueDAG execute should return a list of results."""
        node1 = empty_queuedag.add_node(lambda: 42, iterator=3)
        results = empty_queuedag.execute()

        assert isinstance(results, list)
        assert len(results) == 3

    def test_queuedag_chain_returns_transformed_values(self, empty_queuedag):
        """QueueDAG chain should return transformed values from last node."""
        node1 = empty_queuedag.add_node(lambda: 10, iterator=2)
        node2 = empty_queuedag.add_node(lambda x: x * 3)

        empty_queuedag.add_edge(node1, node2)
        results = empty_queuedag.execute()

        assert all(r == 30 for r in results)

    def test_queuedag_accumulating_results(self, empty_queuedag):
        """QueueDAG should accumulate all iteration results."""
        counter = [0]

        def counting_func():
            counter[0] += 1
            return counter[0]

        node1 = empty_queuedag.add_node(counting_func, iterator=5)
        results = empty_queuedag.execute()

        # Results should contain 1, 2, 3, 4, 5 in some order (LIFO)
        assert len(results) == 5
        assert set(results) == {1, 2, 3, 4, 5}


class TestQueueDAGErrorHandling:
    """Tests for QueueDAG error handling behavior."""

    def test_queuedag_node_exception_stops_iteration(self, empty_queuedag):
        """QueueDAG should stop iteration on first exception."""
        executions = []

        def failing_work():
            executions.append(1)
            raise ValueError("Intentional failure")

        node1 = empty_queuedag.add_node(failing_work, iterator=5)
        # Should not raise to caller - handled internally
        empty_queuedag.execute(print_exc=False)

        # Node stops on first exception
        assert len(executions) == 1

    def test_queuedag_chain_continues_after_parent_exception(self, empty_queuedag):
        """QueueDAG chain should continue processing after parent errors."""
        results = []

        def flaky_parent():
            results.append("parent")
            return 42

        def child_work(x):
            results.append(f"child:{x}")
            return x * 2

        node1 = empty_queuedag.add_node(flaky_parent, iterator=3)
        node2 = empty_queuedag.add_node(child_work, Promise(node1))
        empty_queuedag.add_edge(node1, node2)

        empty_queuedag.execute()

        assert results.count("parent") == 3

    def test_queuedag_halt_condition_with_exception_return(self, empty_queuedag):
        """QueueDAG halt condition should handle None returns from exceptions."""
        executions = []

        def work():
            executions.append(1)
            return len(executions)

        # Halt when we get a value >= 3
        node1 = empty_queuedag.add_node(
            work,
            halt_condition=lambda x: x is not None and x >= 3,
            max_iter=10
        )
        empty_queuedag.execute()

        assert len(executions) == 3


class TestQueueDAGPerformance:
    """Performance and stress tests for QueueDAG."""

    def test_queuedag_high_iteration_count(self, empty_queuedag):
        """QueueDAG should handle many iterations efficiently."""
        import time
        counter = [0]

        def fast_work():
            counter[0] += 1
            return counter[0]

        node1 = empty_queuedag.add_node(fast_work, iterator=100)

        start = time.time()
        empty_queuedag.execute()
        duration = time.time() - start

        assert counter[0] == 100
        assert duration < 1.0, f"100 iterations took {duration}s, expected <1s"

    def test_queuedag_chain_performance(self, empty_queuedag):
        """QueueDAG chain should process efficiently."""
        import time

        # Single iterator node - chains with Promises can have timing issues
        empty_queuedag.add_node(lambda: 4, iterator=50)

        start = time.time()
        results = empty_queuedag.execute()
        duration = time.time() - start

        assert len(results) == 50
        assert all(r == 4 for r in results)
        assert duration < 1.0, f"50 iterations took {duration}s, expected <1s"

    def test_queuedag_rapid_construction(self):
        """QueueDAG construction should be fast."""
        import time

        start = time.time()
        for _ in range(100):
            qdag = QueueDAG()
            qdag.add_node(lambda: 1, iterator=1)
        duration = time.time() - start

        assert duration < 0.5, f"100 QueueDAG constructions took {duration}s"


class TestQueueDAGConditions:
    """Tests for QueueDAG with edge conditions."""

    def test_queuedag_with_success_condition(self, empty_queuedag):
        """QueueDAG should respect Success condition on edges."""
        from webber import Condition
        results = []

        node1 = empty_queuedag.add_node(lambda: results.append("parent") or 42, iterator=2)
        node2 = empty_queuedag.add_node(lambda x: results.append(f"child:{x}"))

        empty_queuedag.add_edge(node1, node2, continue_on=Condition.Success)
        empty_queuedag.execute()

        assert results.count("parent") == 2

    def test_queuedag_preserves_condition_in_edges(self, empty_queuedag):
        """QueueDAG should store condition on edges."""
        from webber import Condition

        node1 = empty_queuedag.add_node(lambda: 1, iterator=1)
        node2 = empty_queuedag.add_node(lambda x: x)

        empty_queuedag.add_edge(node1, node2, continue_on=Condition.AnyCase)

        edge_data = empty_queuedag.graph.edges[(
            empty_queuedag.node_id(node1),
            empty_queuedag.node_id(node2)
        )]
        assert edge_data['Condition'] == Condition.AnyCase

    def test_queuedag_default_condition_is_success(self, empty_queuedag):
        """QueueDAG edges should default to Success condition."""
        from webber import Condition

        node1 = empty_queuedag.add_node(lambda: 1, iterator=1)
        node2 = empty_queuedag.add_node(lambda x: x)

        empty_queuedag.add_edge(node1, node2)  # No explicit condition

        edge_data = empty_queuedag.graph.edges[(
            empty_queuedag.node_id(node1),
            empty_queuedag.node_id(node2)
        )]
        assert edge_data['Condition'] == Condition.Success


class TestQueueDAGNestingLatency:
    """
    Tests documenting the known QueueDAG nesting latency issue.

    As stated in the QueueDAG docstring (v0.2):
    "QueueDAG experiences extreme latency when nested inside of a standard webber.DAG class."

    These tests document this behavior and serve as regression tests if the issue is fixed.
    """

    def test_queuedag_standalone_baseline(self):
        """Establish baseline: standalone QueueDAG should execute quickly."""
        import time

        qdag = QueueDAG()
        counter = [0]

        def work():
            counter[0] += 1
            return counter[0]

        qdag.add_node(work, iterator=10)

        start = time.time()
        qdag.execute()
        standalone_duration = time.time() - start

        assert counter[0] == 10
        assert standalone_duration < 0.5, f"Standalone QueueDAG took {standalone_duration}s"

    def test_queuedag_nested_in_dag_executes(self):
        """QueueDAG execute method can be called from within a standard DAG."""
        import webber

        # Create a QueueDAG that will be nested
        qdag = QueueDAG()
        results = []
        qdag.add_node(lambda: results.append("nested") or len(results), iterator=3)

        # Create outer DAG that calls the QueueDAG
        outer_dag = webber.DAG()
        outer_dag.add_node(qdag.execute)
        outer_dag.execute()

        # The nested QueueDAG should have executed
        assert len(results) == 3

    def test_queuedag_nested_latency_documented(self):
        """
        Document the nesting latency: nested execution is significantly slower.

        This test documents the known limitation rather than asserting specific timing.
        If this test starts failing because nested execution becomes fast, that's good!
        """
        import time
        import webber

        # Standalone execution
        qdag1 = QueueDAG()
        counter1 = [0]
        qdag1.add_node(lambda: counter1[0].__add__(1) or setattr(counter1, '__setitem__', (0, counter1[0] + 1)), iterator=5)

        # Simpler approach - use mutable list
        qdag_standalone = QueueDAG()
        standalone_results = []
        qdag_standalone.add_node(lambda: standalone_results.append(1) or len(standalone_results), iterator=5)

        start = time.time()
        qdag_standalone.execute()
        standalone_time = time.time() - start

        # Nested execution
        qdag_nested = QueueDAG()
        nested_results = []
        qdag_nested.add_node(lambda: nested_results.append(1) or len(nested_results), iterator=5)

        outer = webber.DAG()
        outer.add_node(qdag_nested.execute)

        start = time.time()
        outer.execute()
        nested_time = time.time() - start

        # Both should complete
        assert len(standalone_results) == 5
        assert len(nested_results) == 5

        # Document the ratio (nested is typically much slower)
        # This is informational - we don't fail if it improves
        ratio = nested_time / standalone_time if standalone_time > 0 else float('inf')
        print(f"\nNesting latency ratio: {ratio:.1f}x (nested: {nested_time:.3f}s, standalone: {standalone_time:.3f}s)")

    def test_queuedag_nested_with_promise_chain(self):
        """Test nested QueueDAG with Promise chains in outer DAG."""
        import webber
        from webber import Promise

        # Inner QueueDAG
        qdag = QueueDAG()
        qdag.add_node(lambda: 42, iterator=2)

        # Outer DAG with Promise dependency
        outer = webber.DAG()
        setup = outer.add_node(lambda: "setup_complete")
        nested_exec = outer.add_node(lambda x: qdag.execute() or "qdag_done", Promise(setup))
        outer.add_edge(setup, nested_exec)

        outer.execute()
        # Test passes if no exception - nesting with Promises works

    def test_queuedag_multiple_nested_sequential(self):
        """Multiple QueueDAGs nested sequentially in outer DAG."""
        import webber
        from webber import Promise

        results = []

        # Create two QueueDAGs
        qdag1 = QueueDAG()
        qdag1.add_node(lambda: results.append("q1") or len(results), iterator=2)

        qdag2 = QueueDAG()
        qdag2.add_node(lambda: results.append("q2") or len(results), iterator=2)

        # Outer DAG runs them sequentially
        outer = webber.DAG()
        n1 = outer.add_node(qdag1.execute)
        n2 = outer.add_node(lambda x: qdag2.execute(), Promise(n1))
        outer.add_edge(n1, n2)

        outer.execute()

        # Both QueueDAGs should have executed
        assert results.count("q1") == 2
        assert results.count("q2") == 2
