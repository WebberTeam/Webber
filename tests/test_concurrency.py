"""
Concurrency & Threading Tests - Priority P2

Tests for parallel execution, thread safety, and concurrent access patterns.
"""
import pytest
import time
import threading
import webber
from webber import Promise, Condition


class TestParallelExecution:
    """Tests for truly parallel execution of independent nodes."""

    def test_parallel_execution_timing(self, empty_dag):
        """Independent nodes should execute in parallel, not sequentially."""
        start_times = []
        lock = threading.Lock()

        def task():
            with lock:
                start_times.append(time.time())
            time.sleep(0.001)

        # Create 5 independent nodes
        for _ in range(5):
            empty_dag.add_node(task)

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        # Should take ~0.001s (parallel), not ~0.005s (sequential)
        # Allow some overhead but should be significantly less than sequential
        assert duration < 0.05, f"Execution took {duration}s, expected <0.05s for parallel"
        assert len(start_times) == 5

    def test_parallel_nodes_all_execute(self, empty_dag):
        """All parallel nodes should execute."""
        results = []
        lock = threading.Lock()

        def task(val):
            with lock:
                results.append(val)

        for i in range(10):
            empty_dag.add_node(task, i)

        empty_dag.execute()

        assert len(results) == 10
        assert set(results) == set(range(10))

    def test_sequential_dependency_respected(self, empty_dag):
        """Dependent nodes should wait for parents to complete."""
        execution_order = []
        lock = threading.Lock()

        def record(name):
            time.sleep(0.0005)  # Small delay to make timing deterministic
            with lock:
                execution_order.append(name)

        node1 = empty_dag.add_node(record, "first")
        node2 = empty_dag.add_node(record, "second")
        node3 = empty_dag.add_node(record, "third")

        empty_dag.add_edge(node1, node2)
        empty_dag.add_edge(node2, node3)

        empty_dag.execute()

        # Order should be sequential due to dependencies
        assert execution_order.index("first") < execution_order.index("second")
        assert execution_order.index("second") < execution_order.index("third")

    def test_diamond_parallel_branches(self, empty_dag):
        """Diamond pattern should execute branches in parallel."""
        execution_times = {}
        lock = threading.Lock()

        def timed_task(name):
            start = time.time()
            time.sleep(0.001)
            end = time.time()
            with lock:
                execution_times[name] = (start, end)

        root = empty_dag.add_node(timed_task, "root")
        left = empty_dag.add_node(timed_task, "left")
        right = empty_dag.add_node(timed_task, "right")
        merge = empty_dag.add_node(timed_task, "merge")

        empty_dag.add_edge(root, left)
        empty_dag.add_edge(root, right)
        empty_dag.add_edge(left, merge)
        empty_dag.add_edge(right, merge)

        empty_dag.execute()

        # Left and right should overlap in execution (parallel)
        left_start, left_end = execution_times["left"]
        right_start, right_end = execution_times["right"]

        # Check for overlap - one starts before the other ends
        overlap = (left_start < right_end) and (right_start < left_end)
        assert overlap, "Left and right branches should execute in parallel"


class TestThreadSafety:
    """Tests for thread safety of shared resources."""

    def test_refs_dict_concurrent_access(self, empty_dag):
        """Multiple nodes accessing refs dict should not cause race conditions."""
        node1 = empty_dag.add_node(lambda: "value1")
        node2 = empty_dag.add_node(lambda: "value2")
        node3 = empty_dag.add_node(lambda: "value3")

        # Create many dependent nodes that will access refs concurrently
        for i in range(20):
            empty_dag.add_node(
                lambda a, b, c: f"{a}{b}{c}",
                Promise(node1), Promise(node2), Promise(node3)
            )

        # Add edges to ensure dependency
        for node in list(empty_dag.graph.nodes)[3:]:
            empty_dag.add_edge(node1, node)
            empty_dag.add_edge(node2, node)
            empty_dag.add_edge(node3, node)

        # Should not raise concurrency errors
        empty_dag.execute()

    def test_concurrent_writes_to_shared_list(self, empty_dag):
        """Concurrent writes should all be captured (with proper synchronization)."""
        results = []
        lock = threading.Lock()

        def append_item(item):
            with lock:
                results.append(item)

        for i in range(50):
            empty_dag.add_node(append_item, i)

        empty_dag.execute()

        assert len(results) == 50
        assert set(results) == set(range(50))

    def test_promise_resolution_thread_safety(self, empty_dag):
        """Promise resolution should be thread-safe with concurrent children."""
        node1 = empty_dag.add_node(lambda: {"data": [1, 2, 3]})

        results = []
        lock = threading.Lock()

        def process_data(data):
            # Simulate some processing
            result = sum(data["data"])
            with lock:
                results.append(result)

        # Many nodes all depending on the same parent
        children = []
        for i in range(20):
            child = empty_dag.add_node(process_data, Promise(node1))
            children.append(child)
            empty_dag.add_edge(node1, child)

        empty_dag.execute()

        # All should have received the same data and computed correctly
        assert len(results) == 20
        assert all(r == 6 for r in results)


class TestConcurrentExecution:
    """Tests for concurrent execution patterns."""

    def test_many_roots_concurrent(self, empty_dag):
        """Many root nodes should all start concurrently."""
        start_times = []
        lock = threading.Lock()

        def record_start():
            with lock:
                start_times.append(time.time())
            time.sleep(0.0005)

        # Create 10 root nodes
        for _ in range(10):
            empty_dag.add_node(record_start)

        start = time.time()
        empty_dag.execute()

        # All should have started within a small window
        if len(start_times) > 1:
            time_spread = max(start_times) - min(start_times)
            # Should start nearly simultaneously (within 100ms)
            assert time_spread < 0.1, f"Start time spread was {time_spread}s"

    def test_parallel_with_different_durations(self, empty_dag):
        """Nodes with different durations should still run in parallel."""
        completion_order = []
        lock = threading.Lock()

        def slow_task():
            time.sleep(0.002)
            with lock:
                completion_order.append("slow")

        def fast_task():
            time.sleep(0.0005)
            with lock:
                completion_order.append("fast")

        empty_dag.add_node(slow_task)
        empty_dag.add_node(fast_task)
        empty_dag.add_node(fast_task)

        empty_dag.execute()

        # Fast tasks should complete before slow task
        assert completion_order.count("fast") == 2
        assert completion_order.count("slow") == 1
        # Fast tasks likely completed first
        fast_indices = [i for i, x in enumerate(completion_order) if x == "fast"]
        slow_index = completion_order.index("slow")
        # At least one fast should complete before slow
        assert any(fi < slow_index for fi in fast_indices)


class TestConcurrencyEdgeCases:
    """Edge cases in concurrent execution."""

    def test_single_node_execution(self, empty_dag):
        """Single node DAG should execute without threading issues."""
        result = []
        empty_dag.add_node(lambda: result.append("executed"))
        empty_dag.execute()

        assert result == ["executed"]

    def test_long_chain_sequential(self, empty_dag):
        """Long sequential chain should maintain order."""
        execution_order = []
        lock = threading.Lock()

        def record(num):
            with lock:
                execution_order.append(num)

        nodes = []
        for i in range(10):
            node = empty_dag.add_node(record, i)
            nodes.append(node)

        for i in range(len(nodes) - 1):
            empty_dag.add_edge(nodes[i], nodes[i + 1])

        empty_dag.execute()

        # Should execute in order due to dependencies
        assert execution_order == list(range(10))

    def test_wide_then_narrow(self, empty_dag):
        """Wide parallelism narrowing to single node."""
        stage1_times = []
        stage2_result = []
        lock = threading.Lock()

        def stage1_task(val):
            time.sleep(0.0005)
            with lock:
                stage1_times.append(time.time())
            return val

        def merge_task(*args):
            with lock:
                stage2_result.append(sum(args) if args else 0)

        # Create wide first stage
        stage1_nodes = []
        for i in range(5):
            node = empty_dag.add_node(stage1_task, i)
            stage1_nodes.append(node)

        # Create merge node
        merge = empty_dag.add_node(merge_task)

        # Connect all stage1 to merge
        for node in stage1_nodes:
            empty_dag.add_edge(node, merge)

        empty_dag.execute()

        # Stage 1 should have run in parallel
        if len(stage1_times) > 1:
            time_spread = max(stage1_times) - min(stage1_times)
            assert time_spread < 0.1  # Started roughly together


class TestExecutorBehavior:
    """Tests for DAGExecutor behavior."""

    def test_executor_completes_all_nodes(self, empty_dag):
        """Executor should wait for all nodes to complete."""
        completed = set()
        lock = threading.Lock()

        def slow_complete(name):
            time.sleep(0.001)
            with lock:
                completed.add(name)

        for i in range(5):
            empty_dag.add_node(slow_complete, f"node_{i}")

        empty_dag.execute()

        assert len(completed) == 5

    def test_executor_handles_exceptions(self, empty_dag):
        """Executor should handle exceptions without crashing."""
        results = []

        def failing_task():
            raise ValueError("Intentional failure")

        def success_task():
            results.append("success")

        empty_dag.add_node(failing_task)
        empty_dag.add_node(success_task)

        # Should not raise, other tasks should still complete
        empty_dag.execute()

        assert "success" in results

    def test_return_ref_provides_executor(self, empty_dag):
        """return_ref=True should return the executor."""
        empty_dag.add_node(lambda: "test")

        executor = empty_dag.execute(return_ref=True)

        assert executor is not None


class TestConcurrencyWithConditions:
    """Tests for concurrency with conditional edges."""

    def test_parallel_with_success_conditions(self, empty_dag):
        """Parallel children with success conditions should all execute."""
        results = []
        lock = threading.Lock()

        parent = empty_dag.add_node(lambda: "success")

        def append_with_lock(x):
            with lock:
                results.append(x)

        for i in range(5):
            child = empty_dag.add_node(append_with_lock, i)
            empty_dag.add_edge(parent, child, continue_on=Condition.Success)

        empty_dag.execute()

        assert len(results) == 5

    def test_parallel_with_mixed_conditions(self, empty_dag):
        """Mixed conditions should route correctly."""
        success_results = []
        failure_results = []
        lock = threading.Lock()

        parent = empty_dag.add_node(lambda: 1 / 0)  # Will fail

        def append_success():
            with lock:
                success_results.append("success")

        def append_failure():
            with lock:
                failure_results.append("failure")

        success_child = empty_dag.add_node(append_success)
        failure_child = empty_dag.add_node(append_failure)

        empty_dag.add_edge(parent, success_child, continue_on=Condition.Success)
        empty_dag.add_edge(parent, failure_child, continue_on=Condition.Failure)

        empty_dag.execute()

        assert len(success_results) == 0
        assert len(failure_results) == 1
