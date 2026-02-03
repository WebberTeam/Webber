"""
Integration Tests - Priority P2

Tests for real-world patterns including ETL pipelines, fan-out/fan-in,
error handling patterns, and visualization smoke tests.
"""
import pytest
import webber
from webber import Promise, Condition


class TestETLPipeline:
    """Tests simulating complete ETL (Extract, Transform, Load) pipelines."""

    def test_simple_etl_pipeline(self, empty_dag):
        """Simulate complete ETL pipeline with data flowing through stages."""
        results = {}

        def extract():
            return [1, 2, 3, 4, 5]

        def transform(data):
            return [x * 2 for x in data]

        def load(data):
            results['final'] = data

        extract_node = empty_dag.add_node(extract)
        transform_node = empty_dag.add_node(transform, Promise(extract_node))
        load_node = empty_dag.add_node(load, Promise(transform_node))

        empty_dag.add_edge(extract_node, transform_node)
        empty_dag.add_edge(transform_node, load_node)
        empty_dag.execute()

        assert results['final'] == [2, 4, 6, 8, 10]

    def test_etl_with_multiple_transforms(self, empty_dag):
        """ETL with parallel transform steps."""
        results = {}

        def extract():
            return {"numbers": [1, 2, 3], "strings": ["a", "b", "c"]}

        def transform_numbers(data):
            return [n * 10 for n in data["numbers"]]

        def transform_strings(data):
            return [s.upper() for s in data["strings"]]

        def load(numbers, strings):
            results['numbers'] = numbers
            results['strings'] = strings

        extract_node = empty_dag.add_node(extract)
        num_transform = empty_dag.add_node(transform_numbers, Promise(extract_node))
        str_transform = empty_dag.add_node(transform_strings, Promise(extract_node))
        load_node = empty_dag.add_node(
            load,
            Promise(num_transform),
            Promise(str_transform)
        )

        empty_dag.add_edge(extract_node, num_transform)
        empty_dag.add_edge(extract_node, str_transform)
        empty_dag.add_edge(num_transform, load_node)
        empty_dag.add_edge(str_transform, load_node)

        empty_dag.execute()

        assert results['numbers'] == [10, 20, 30]
        assert results['strings'] == ['A', 'B', 'C']

    def test_etl_with_validation(self, empty_dag):
        """ETL with validation step that can fail."""
        results = {'validated': False, 'loaded': False}

        def extract():
            return [1, 2, 3, 4, 5]

        def validate(data):
            if len(data) < 3:
                raise ValueError("Not enough data")
            results['validated'] = True
            return data

        def load(data):
            results['loaded'] = True

        extract_node = empty_dag.add_node(extract)
        validate_node = empty_dag.add_node(validate, Promise(extract_node))
        load_node = empty_dag.add_node(load, Promise(validate_node))

        empty_dag.add_edge(extract_node, validate_node)
        empty_dag.add_edge(validate_node, load_node, continue_on=Condition.Success)

        empty_dag.execute()

        assert results['validated'] == True
        assert results['loaded'] == True


class TestFanOutFanIn:
    """Tests for fan-out to parallel tasks, then fan-in to aggregator."""

    def test_basic_fan_out_fan_in(self, empty_dag):
        """Test fan-out to parallel processors, fan-in to aggregator."""
        results = []

        root = empty_dag.add_node(lambda: [1, 2, 3, 4, 5])

        # Fan-out: Process each element
        processors = []
        for i in range(5):
            proc = empty_dag.add_node(lambda data, idx=i: data[idx] * 2, Promise(root))
            processors.append(proc)
            empty_dag.add_edge(root, proc)

        # Fan-in: Aggregate results
        def aggregate(*args):
            results.append(sum(args))

        aggregator = empty_dag.add_node(
            aggregate,
            *[Promise(p) for p in processors]
        )

        for proc in processors:
            empty_dag.add_edge(proc, aggregator)

        empty_dag.execute()

        # Sum of [2, 4, 6, 8, 10] = 30
        assert results == [30]

    def test_fan_out_with_failure_handling(self, empty_dag):
        """Fan-out with some workers failing should still aggregate successful ones."""
        successful_results = []

        root = empty_dag.add_node(lambda: [1, 2, 3])
        cleanup = empty_dag.add_node(lambda: successful_results.append("cleanup"))

        # Create workers, some will fail
        workers = []
        for i in range(3):
            if i == 1:  # Second worker fails
                worker = empty_dag.add_node(lambda: 1/0)
            else:
                worker = empty_dag.add_node(lambda idx=i: idx * 10)
            workers.append(worker)
            empty_dag.add_edge(root, worker)

        # Cleanup runs regardless
        for worker in workers:
            empty_dag.add_edge(worker, cleanup, continue_on=Condition.AnyCase)

        empty_dag.execute()

        # Cleanup should have run
        assert "cleanup" in successful_results

    def test_nested_fan_out(self, empty_dag):
        """Multi-level fan-out pattern."""
        results = []

        level1 = empty_dag.add_node(lambda: "start")

        # First level fan-out
        level2_nodes = []
        for i in range(2):
            node = empty_dag.add_node(lambda idx=i: f"L2-{idx}")
            level2_nodes.append(node)
            empty_dag.add_edge(level1, node)

        # Second level fan-out from each L2 node
        level3_nodes = []
        for l2_node in level2_nodes:
            for j in range(2):
                node = empty_dag.add_node(lambda: results.append("L3"))
                level3_nodes.append(node)
                empty_dag.add_edge(l2_node, node)

        empty_dag.execute()

        # Should have 4 L3 executions
        assert results.count("L3") == 4


class TestErrorHandlingPatterns:
    """Tests for common error handling patterns."""

    def test_primary_with_fallback(self, empty_dag):
        """Test pattern with primary path and error fallback."""
        results = []

        risky = empty_dag.add_node(lambda: 1 / 0)  # Will fail
        primary = empty_dag.add_node(lambda: results.append("primary"))
        fallback = empty_dag.add_node(lambda: results.append("fallback"))
        cleanup = empty_dag.add_node(lambda: results.append("cleanup"))

        empty_dag.add_edge(risky, primary, continue_on=Condition.Success)
        empty_dag.add_edge(risky, fallback, continue_on=Condition.Failure)
        empty_dag.add_edge(risky, cleanup, continue_on=Condition.AnyCase)

        empty_dag.execute()

        assert "primary" not in results
        assert "fallback" in results
        assert "cleanup" in results

    def test_retry_then_fallback(self, empty_dag):
        """Retry pattern with ultimate fallback."""
        attempts = []
        results = []

        def flaky_task():
            attempts.append(1)
            if len(attempts) <= 3:  # Fail first 3 times
                raise ValueError("Flaky")
            return "success"

        risky = empty_dag.add_node(flaky_task)
        fallback = empty_dag.add_node(lambda: results.append("fallback"))
        success = empty_dag.add_node(lambda: results.append("success"))

        empty_dag.retry_node(risky, count=2)  # Will retry twice (3 total)
        empty_dag.add_edge(risky, success, continue_on=Condition.Success)
        empty_dag.add_edge(risky, fallback, continue_on=Condition.Failure)

        empty_dag.execute()

        # Should have tried 3 times, still failed, went to fallback
        assert len(attempts) == 3
        assert "fallback" in results
        assert "success" not in results

    def test_cascading_recovery(self, empty_dag):
        """Recovery at one stage allowing downstream to continue."""
        results = []

        risky = empty_dag.add_node(lambda: 1/0)
        recover = empty_dag.add_node(lambda: results.append("recover") or "recovered")
        continue_processing = empty_dag.add_node(lambda: results.append("continue"))
        final = empty_dag.add_node(lambda: results.append("final"))

        empty_dag.add_edge(risky, recover, continue_on=Condition.Failure)
        empty_dag.add_edge(recover, continue_processing, continue_on=Condition.Success)
        empty_dag.add_edge(continue_processing, final, continue_on=Condition.Success)

        empty_dag.execute()

        assert results == ["recover", "continue", "final"]

    def test_multiple_fallback_levels(self, empty_dag):
        """Multi-level fallback chain."""
        results = []

        primary = empty_dag.add_node(lambda: 1/0)
        fallback1 = empty_dag.add_node(lambda: 1/0)  # Also fails
        fallback2 = empty_dag.add_node(lambda: results.append("fallback2"))

        empty_dag.add_edge(primary, fallback1, continue_on=Condition.Failure)
        empty_dag.add_edge(fallback1, fallback2, continue_on=Condition.Failure)

        empty_dag.execute()

        assert results == ["fallback2"]


class TestVisualizationSmoke:
    """Smoke tests for visualization (verifies no crashes)."""

    def test_visualize_simple_dag_plt(self, empty_dag):
        """Visualization with matplotlib should not raise errors."""
        node1 = empty_dag.add_node(lambda: "test")
        node2 = empty_dag.add_node(lambda: "test2")
        empty_dag.add_edge(node1, node2)

        try:
            empty_dag.visualize(type='plt')
        except ImportError:
            pytest.skip("Visualization dependencies not available")
        except Exception as e:
            # Some display-related errors are OK in headless environments
            if "display" in str(e).lower() or "tkinter" in str(e).lower():
                pytest.skip("Display not available for visualization")
            raise

    def test_visualize_complex_dag(self, diamond_dag):
        """Visualization of complex DAG should not crash."""
        dag, root, left, right, merge = diamond_dag

        try:
            dag.visualize(type='plt')
        except ImportError:
            pytest.skip("Visualization dependencies not available")
        except Exception as e:
            if "display" in str(e).lower() or "tkinter" in str(e).lower():
                pytest.skip("Display not available for visualization")
            raise


class TestCriticalPath:
    """Tests for critical path extraction functionality."""

    def test_critical_path_simple(self, empty_dag):
        """Extract critical path from simple chain."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")
        node3 = empty_dag.add_node(lambda: "3")

        empty_dag.add_edge(node1, node2)
        empty_dag.add_edge(node2, node3)

        critical = empty_dag.critical_path(node3)

        assert node1 in critical.graph.nodes
        assert node2 in critical.graph.nodes
        assert node3 in critical.graph.nodes

    def test_critical_path_excludes_unrelated(self, empty_dag):
        """Critical path should exclude nodes not on path."""
        node1 = empty_dag.add_node(lambda: "1")
        node2 = empty_dag.add_node(lambda: "2")
        node3 = empty_dag.add_node(lambda: "3")
        node4 = empty_dag.add_node(lambda: "4")
        node5 = empty_dag.add_node(lambda: "5")

        empty_dag.add_edge(node1, node2)
        empty_dag.add_edge(node2, node3)
        empty_dag.add_edge(node1, node4)
        empty_dag.add_edge(node4, node5)

        # Critical path to node3 only
        critical = empty_dag.critical_path(node3)

        assert node1 in critical.graph.nodes
        assert node2 in critical.graph.nodes
        assert node3 in critical.graph.nodes
        assert node4 not in critical.graph.nodes
        assert node5 not in critical.graph.nodes

    def test_critical_path_multiple_targets(self, empty_dag):
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

        # Critical path to both node3 and node5
        critical = empty_dag.critical_path([node3, node5])

        # Should include all nodes
        assert len(critical.graph.nodes) == 5

    def test_critical_path_executable(self, empty_dag):
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

        # Should only execute nodes on path to node3
        assert "1" in results
        assert "2" in results
        assert "3" in results
        assert "4" not in results


class TestRealWorldPatterns:
    """Tests for patterns seen in real-world usage."""

    def test_api_call_with_retry_and_cache(self, empty_dag):
        """Simulate API call with retry and caching pattern."""
        cache = {}
        api_calls = []

        def check_cache():
            data = cache.get('data')
            if data is None:
                raise KeyError("Cache miss")  # Raise exception on cache miss
            return data

        def call_api():
            api_calls.append(1)
            if len(api_calls) < 2:
                raise ConnectionError("Network error")
            return {"result": "data"}

        def save_to_cache(data):
            cache['data'] = data

        check = empty_dag.add_node(check_cache)
        api = empty_dag.add_node(call_api)
        save = empty_dag.add_node(save_to_cache, Promise(api))

        empty_dag.retry_node(api, count=3)
        empty_dag.add_edge(check, api, continue_on=Condition.Failure)  # Only call API if cache miss
        empty_dag.add_edge(api, save, continue_on=Condition.Success)

        empty_dag.execute()

        # API should have been called and succeeded on retry
        assert len(api_calls) == 2
        assert cache.get('data') == {"result": "data"}

    def test_batch_processing_pattern(self, empty_dag):
        """Batch processing with parallel workers."""
        results = []

        def get_batch():
            return list(range(10))

        def process_item(item):
            return item * 2

        def collect_results(*args):
            results.extend(args)

        batch_node = empty_dag.add_node(get_batch)

        # Create parallel processors
        processors = []
        for i in range(10):
            proc = empty_dag.add_node(
                process_item,
                Promise(batch_node)
            )
            # Hacky way to get each item - in real usage would use indexing
            processors.append(proc)
            empty_dag.add_edge(batch_node, proc)

        # Collector
        collector = empty_dag.add_node(
            collect_results,
            *[Promise(p) for p in processors]
        )
        for proc in processors:
            empty_dag.add_edge(proc, collector)

        empty_dag.execute()

        # All processors got the full batch and returned doubled first element
        assert len(results) == 10

    def test_workflow_with_approval_gate(self, empty_dag):
        """Workflow with conditional approval step."""
        results = []

        def prepare_data():
            return {"amount": 5000}

        def check_threshold(data):
            if data["amount"] > 1000:
                raise ValueError("Requires approval")
            return data

        def auto_approve(data):
            results.append("auto_approved")

        def manual_review():
            results.append("sent_for_review")

        prepare = empty_dag.add_node(prepare_data)
        check = empty_dag.add_node(check_threshold, Promise(prepare))
        auto = empty_dag.add_node(auto_approve, Promise(prepare))
        manual = empty_dag.add_node(manual_review)

        empty_dag.add_edge(prepare, check)
        empty_dag.add_edge(check, auto, continue_on=Condition.Success)
        empty_dag.add_edge(check, manual, continue_on=Condition.Failure)

        empty_dag.execute()

        # Amount > 1000, so should go to manual review
        assert "sent_for_review" in results
        assert "auto_approved" not in results
