"""
Performance & Stress Tests - Priority P3

Tests for latency, memory handling, and stress scenarios.
These tests help ensure Webber maintains its no-latency promise.
"""
import pytest
import time
import webber
from webber import Promise, Condition


class TestExecutionLatency:
    """Tests for measuring execution overhead."""

    def test_single_node_latency(self, empty_dag):
        """Webber overhead for single node should be sub-100ms."""
        node1 = empty_dag.add_node(lambda: 42)

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        # Should be sub-100ms for single trivial node
        assert duration < 0.1, f"Execution took {duration}s, expected <0.1s"

    def test_empty_dag_latency(self, empty_dag):
        """Empty DAG execution should be nearly instant."""
        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        # Should be nearly instant
        assert duration < 0.05, f"Empty DAG took {duration}s"

    def test_overhead_vs_direct_execution(self, empty_dag):
        """Webber overhead should be minimal compared to direct execution."""
        def work():
            total = 0
            for i in range(1000):
                total += i
            return total

        # Direct execution
        start = time.time()
        for _ in range(10):
            work()
        direct_time = time.time() - start

        # Webber execution
        for _ in range(10):
            empty_dag.add_node(work)

        start = time.time()
        empty_dag.execute()
        webber_time = time.time() - start

        # Overhead should be reasonable
        # Parallel execution may actually be faster than sequential direct
        # Use max() to account for fixed framework overhead on very fast workloads
        threshold = max(direct_time * 10, 0.05)  # At least 50ms for framework overhead
        assert webber_time < threshold, \
            f"Webber: {webber_time}s vs Direct: {direct_time}s (threshold: {threshold}s)"

    def test_promise_resolution_latency(self, empty_dag):
        """Promise resolution should add minimal overhead."""
        node1 = empty_dag.add_node(lambda: 42)
        node2 = empty_dag.add_node(lambda x: x * 2, Promise(node1))

        empty_dag.add_edge(node1, node2)

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        # Two nodes with promise should still be fast
        assert duration < 0.2, f"Promise resolution took {duration}s"

    def test_condition_checking_latency(self, empty_dag):
        """Condition checking should not add significant overhead."""
        results = []
        parent = empty_dag.add_node(lambda: "success")

        for i in range(20):
            child = empty_dag.add_node(lambda idx=i: results.append(idx))
            empty_dag.add_edge(parent, child, continue_on=Condition.Success)

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        # Should be fast despite 20 condition checks
        assert duration < 0.5, f"Condition checking took {duration}s"
        assert len(results) == 20


class TestMemoryHandling:
    """Tests for memory usage with large structures."""

    def test_large_node_count(self, empty_dag):
        """DAG should handle thousands of nodes."""
        # Create 1000 nodes
        nodes = [empty_dag.add_node(lambda i=i: i) for i in range(1000)]

        assert len(empty_dag.graph.nodes) == 1000

    def test_large_node_count_execution(self, empty_dag):
        """Executing DAG with many nodes should complete."""
        # Create 500 independent nodes
        for i in range(500):
            empty_dag.add_node(lambda i=i: i)

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        # Should complete in reasonable time
        assert duration < 10, f"Large DAG execution took {duration}s"

    def test_large_data_through_promise(self, empty_dag):
        """Promises should handle large data structures."""
        # Generate large list
        node1 = empty_dag.add_node(lambda: list(range(10000)))
        node2 = empty_dag.add_node(lambda x: len(x), Promise(node1))

        empty_dag.add_edge(node1, node2)

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        # Should handle large data reasonably
        assert duration < 2, f"Large data promise took {duration}s"

    def test_many_promises_to_same_node(self, empty_dag):
        """Many promises referencing same node should work efficiently."""
        source = empty_dag.add_node(lambda: {"data": "value"})

        # Create 50 nodes all referencing the same source
        children = []
        for i in range(50):
            child = empty_dag.add_node(lambda d, idx=i: d["data"], Promise(source))
            children.append(child)
            empty_dag.add_edge(source, child)

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        assert duration < 2, f"Many promises took {duration}s"


class TestStressScenarios:
    """Stress tests for edge case scenarios."""

    def test_rapid_dag_construction(self, empty_dag):
        """Building large DAG should be fast."""
        start = time.time()

        for i in range(500):
            empty_dag.add_node(lambda i=i: i)

        duration = time.time() - start

        # Should be able to add 500 nodes quickly
        assert duration < 1.0, f"Construction took {duration}s"

    def test_rapid_edge_construction(self, empty_dag):
        """Adding many edges should be reasonably fast."""
        # Create nodes first
        nodes = [empty_dag.add_node(lambda i=i: i) for i in range(20)]

        start = time.time()

        # Add edges in topological order
        edge_count = 0
        for i in range(len(nodes) - 1):
            for j in range(i + 1, min(i + 5, len(nodes))):
                empty_dag.add_edge(nodes[i], nodes[j])
                edge_count += 1

        duration = time.time() - start

        assert duration < 2.0, f"Edge construction took {duration}s for {edge_count} edges"

    def test_complex_diamond_patterns(self, empty_dag):
        """Complex DAG with many diamond patterns should execute correctly."""
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

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        assert len(results) == 40  # 4 nodes per pattern * 10 patterns
        assert duration < 5, f"Complex patterns took {duration}s"

    def test_deep_chain_execution(self, empty_dag):
        """Very long sequential chain should execute correctly."""
        results = []

        nodes = []
        for i in range(100):
            node = empty_dag.add_node(lambda idx=i: results.append(idx) or idx)
            nodes.append(node)

        for i in range(len(nodes) - 1):
            empty_dag.add_edge(nodes[i], nodes[i + 1])

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        assert len(results) == 100
        assert results == list(range(100))  # Should be in order
        assert duration < 5, f"Deep chain took {duration}s"

    def test_wide_parallel_execution(self, empty_dag):
        """Very wide parallel execution should complete efficiently."""
        results = []

        root = empty_dag.add_node(lambda: "root")

        # Create 100 parallel children
        for i in range(100):
            child = empty_dag.add_node(lambda idx=i: results.append(idx))
            empty_dag.add_edge(root, child)

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        assert len(results) == 100
        # Parallel execution should be relatively fast
        assert duration < 2, f"Wide parallel took {duration}s"


class TestRetryPerformance:
    """Tests for retry mechanism performance."""

    def test_retry_does_not_slow_success(self, empty_dag):
        """Setting retry count should not slow down successful execution."""
        results = []

        for i in range(20):
            node = empty_dag.add_node(lambda idx=i: results.append(idx))
            empty_dag.retry_node(node, count=5)

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        assert len(results) == 20
        assert duration < 1, f"Retry setup slowed execution: {duration}s"

    def test_retry_exhaustion_performance(self, empty_dag):
        """Retry exhaustion should not hang indefinitely."""
        attempts = []

        def always_fails():
            attempts.append(1)
            raise ValueError("Always fails")

        node = empty_dag.add_node(always_fails)
        empty_dag.retry_node(node, count=5)

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        assert len(attempts) == 6  # Initial + 5 retries
        assert duration < 2, f"Retry exhaustion took {duration}s"


class TestSkipPerformance:
    """Tests for skip mechanism performance."""

    def test_skip_avoids_execution(self, empty_dag):
        """Skipped nodes should not consume execution time."""
        results = []

        # Create slow nodes but skip them
        for i in range(10):
            node = empty_dag.add_node(lambda: time.sleep(0.5))
            empty_dag.skip_node(node, skip=True)

        # Add one non-skipped node
        empty_dag.add_node(lambda: results.append("executed"))

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        assert "executed" in results
        # Should be fast since skipped nodes don't sleep
        assert duration < 1, f"Skipped nodes still took time: {duration}s"


class TestScalability:
    """Tests for scalability characteristics."""

    @pytest.mark.parametrize("node_count", [10, 50, 100])
    def test_linear_scaling_independent_nodes(self, empty_dag, node_count):
        """Independent nodes should scale reasonably."""
        for i in range(node_count):
            empty_dag.add_node(lambda i=i: i)

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        # Allow generous time but ensure completion
        assert duration < node_count * 0.1, f"{node_count} nodes took {duration}s"

    @pytest.mark.parametrize("depth", [5, 10, 20])
    def test_chain_depth_scaling(self, empty_dag, depth):
        """Chain depth should not cause exponential slowdown."""
        results = []

        nodes = [empty_dag.add_node(lambda: results.append(1)) for _ in range(depth)]

        for i in range(len(nodes) - 1):
            empty_dag.add_edge(nodes[i], nodes[i + 1])

        start = time.time()
        empty_dag.execute()
        duration = time.time() - start

        assert len(results) == depth
        # Should scale roughly linearly
        assert duration < depth * 0.2, f"Depth {depth} took {duration}s"


class TestCriticalPathPerformance:
    """Tests for critical path extraction performance."""

    def test_critical_path_extraction_speed(self, empty_dag):
        """Critical path extraction should be fast."""
        # Build a moderately complex DAG
        nodes = [empty_dag.add_node(lambda i=i: i) for i in range(50)]

        for i in range(len(nodes) - 1):
            empty_dag.add_edge(nodes[i], nodes[i + 1])

        start = time.time()
        critical = empty_dag.critical_path(nodes[-1])
        duration = time.time() - start

        assert len(critical.graph.nodes) == 50
        assert duration < 0.5, f"Critical path took {duration}s"

    def test_critical_path_subgraph_execution(self, empty_dag):
        """Critical path subgraph should execute efficiently."""
        results = []

        nodes = [empty_dag.add_node(lambda i=i: results.append(i)) for i in range(30)]

        for i in range(len(nodes) - 1):
            empty_dag.add_edge(nodes[i], nodes[i + 1])

        critical = empty_dag.critical_path(nodes[-1])

        start = time.time()
        critical.execute()
        duration = time.time() - start

        assert len(results) == 30
        assert duration < 2, f"Critical path execution took {duration}s"


class TestExtremeStress:
    """Extreme stress tests requiring subsecond execution."""

    def test_massive_parallel_fanout(self, empty_dag):
        """200 parallel nodes must complete in under 1 second."""
        import threading
        results = []
        lock = threading.Lock()

        root = empty_dag.add_node(lambda: 'start')
        for i in range(200):
            def work(idx=i):
                with lock:
                    results.append(idx)
                return idx
            child = empty_dag.add_node(work)
            empty_dag.add_edge(root, child)

        start = time.time()
        empty_dag.execute(verbose=False)
        duration = time.time() - start

        assert len(results) == 200, f"Expected 200 results, got {len(results)}"
        assert duration < 1.0, f"200 parallel nodes took {duration}s, expected <1s"

    def test_deep_sequential_chain(self, empty_dag):
        """100-node sequential chain must complete in under 1 second."""
        chain_results = []

        prev = empty_dag.add_node(lambda: chain_results.append(0) or 0)
        for i in range(1, 100):
            curr = empty_dag.add_node(lambda x, idx=i: chain_results.append(idx) or idx, Promise(prev))
            empty_dag.add_edge(prev, curr)
            prev = curr

        start = time.time()
        empty_dag.execute(verbose=False)
        duration = time.time() - start

        assert chain_results == list(range(100)), "Chain executed out of order"
        assert duration < 1.0, f"100-node chain took {duration}s, expected <1s"

    def test_diamond_pattern_stress(self, empty_dag):
        """50 diamond patterns (200 nodes) must complete in under 1 second."""
        import threading
        diamond_results = []
        lock = threading.Lock()

        for d in range(50):
            root = empty_dag.add_node(lambda idx=d: f'root{idx}')
            left = empty_dag.add_node(lambda idx=d: f'left{idx}')
            right = empty_dag.add_node(lambda idx=d: f'right{idx}')

            def merge_fn(idx=d):
                with lock:
                    diamond_results.append(f'merge{idx}')
            merge = empty_dag.add_node(merge_fn)

            empty_dag.add_edge(root, left)
            empty_dag.add_edge(root, right)
            empty_dag.add_edge(left, merge)
            empty_dag.add_edge(right, merge)

        start = time.time()
        empty_dag.execute(verbose=False)
        duration = time.time() - start

        assert len(diamond_results) == 50, f"Expected 50 merges, got {len(diamond_results)}"
        assert duration < 1.0, f"50 diamonds took {duration}s, expected <1s"

    def test_retry_stress(self, empty_dag):
        """20 nodes each failing twice must complete in under 1 second."""
        import threading
        attempt_counts = {}
        lock = threading.Lock()

        for i in range(20):
            def flaky(idx=i):
                with lock:
                    attempt_counts[idx] = attempt_counts.get(idx, 0) + 1
                    if attempt_counts[idx] < 3:
                        raise ValueError(f'Fail {idx}')
                return f'success{idx}'
            node = empty_dag.add_node(flaky)
            empty_dag.retry_node(node, count=3)

        start = time.time()
        empty_dag.execute(verbose=False)
        duration = time.time() - start

        assert all(v >= 3 for v in attempt_counts.values()), "Retries not working"
        assert duration < 1.0, f"Retry stress took {duration}s, expected <1s"

    def test_mixed_conditions_stress(self, empty_dag):
        """50 success/failure condition routings must complete in under 1 second."""
        import threading
        success_count = [0]
        failure_count = [0]
        lock = threading.Lock()

        for i in range(50):
            if i % 2 == 0:
                parent = empty_dag.add_node(lambda: 'ok')
            else:
                parent = empty_dag.add_node(lambda: 1/0)

            def on_success():
                with lock:
                    success_count[0] += 1

            def on_failure():
                with lock:
                    failure_count[0] += 1

            s_child = empty_dag.add_node(on_success)
            f_child = empty_dag.add_node(on_failure)
            empty_dag.add_edge(parent, s_child, continue_on=Condition.Success)
            empty_dag.add_edge(parent, f_child, continue_on=Condition.Failure)

        start = time.time()
        empty_dag.execute(verbose=False)
        duration = time.time() - start

        assert success_count[0] == 25, f"Expected 25 successes, got {success_count[0]}"
        assert failure_count[0] == 25, f"Expected 25 failures, got {failure_count[0]}"
        assert duration < 1.0, f"Mixed conditions took {duration}s, expected <1s"

    def test_large_data_transfer(self, empty_dag):
        """1M integers through promise must complete in under 1 second."""
        def produce():
            return list(range(1_000_000))

        def consume(data):
            return sum(data)

        p = empty_dag.add_node(produce)
        c = empty_dag.add_node(consume, Promise(p))
        empty_dag.add_edge(p, c)

        start = time.time()
        empty_dag.execute(verbose=False)
        duration = time.time() - start

        assert duration < 1.0, f"1M data transfer took {duration}s, expected <1s"

    def test_rapid_dag_construction_1000(self, empty_dag):
        """Creating 1000 nodes must complete in under 1 second."""
        start = time.time()
        for i in range(1000):
            empty_dag.add_node(lambda i=i: i)
        duration = time.time() - start

        assert len(empty_dag.graph.nodes) == 1000
        assert duration < 1.0, f"1000 node construction took {duration}s, expected <1s"

    def test_500_parallel_workers(self, empty_dag):
        """500 parallel workers must complete in under 1 second."""
        import threading
        results = []
        lock = threading.Lock()

        root = empty_dag.add_node(lambda: 'go')
        for i in range(500):
            def worker(idx=i):
                with lock:
                    results.append(idx)
                return idx
            child = empty_dag.add_node(worker)
            empty_dag.add_edge(root, child)

        start = time.time()
        empty_dag.execute(verbose=False)
        duration = time.time() - start

        assert len(results) == 500, f"Expected 500 results, got {len(results)}"
        assert duration < 1.0, f"500 workers took {duration}s, expected <1s"

    def test_rapid_dag_executions(self):
        """100 rapid DAG executions must complete in under 1 second."""
        start = time.time()
        for i in range(100):
            dag = webber.DAG()
            n1 = dag.add_node(lambda: 1)
            n2 = dag.add_node(lambda x: x+1, Promise(n1))
            dag.add_edge(n1, n2)
            dag.execute(verbose=False)
        duration = time.time() - start

        assert duration < 1.0, f"100 DAG executions took {duration}s, expected <1s"

    def test_wide_fanin_aggregation(self, empty_dag):
        """100 sources aggregating to 1 node must complete in under 1 second."""
        sources = [empty_dag.add_node(lambda i=i: i) for i in range(100)]

        def aggregator(*args):
            return sum(args)

        agg = empty_dag.add_node(aggregator, *[Promise(s) for s in sources])
        for s in sources:
            empty_dag.add_edge(s, agg)

        start = time.time()
        empty_dag.execute(verbose=False)
        duration = time.time() - start

        assert duration < 1.0, f"100->1 fanin took {duration}s, expected <1s"

    def test_critical_path_500_nodes(self, empty_dag):
        """Critical path extraction from 500 nodes must complete in under 1 second."""
        nodes = [empty_dag.add_node(lambda i=i: i) for i in range(500)]
        for i in range(len(nodes) - 1):
            empty_dag.add_edge(nodes[i], nodes[i+1])

        start = time.time()
        critical = empty_dag.critical_path(nodes[-1])
        duration = time.time() - start

        assert len(critical.graph.nodes) == 500
        assert duration < 1.0, f"500-node critical path took {duration}s, expected <1s"
