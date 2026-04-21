"""Tests for DAGMetrics execution telemetry."""
import time
import pytest
import webber
from webber import DAG, DAGMetrics, Promise
from webber.edges import Condition


class TestMetricsBasic:
    """Basic metrics collection from DAG execution."""

    def test_metrics_available_after_execution(self):
        """Executor should expose metrics property after run."""
        dag = DAG()
        dag.add_node(lambda: "hello")
        executor = dag.execute(return_ref=True)

        m = executor.metrics
        assert isinstance(m, DAGMetrics)

    def test_total_duration_positive(self):
        """Total duration should be a positive float."""
        dag = DAG()
        dag.add_node(lambda: time.sleep(0.01))
        executor = dag.execute(return_ref=True)

        assert executor.metrics.total_duration > 0

    def test_total_nodes_count(self):
        """total_nodes should reflect the DAG size."""
        dag = DAG()
        dag.add_node(lambda: "a")
        dag.add_node(lambda: "b")
        dag.add_node(lambda: "c")
        executor = dag.execute(return_ref=True)

        assert executor.metrics.total_nodes == 3

    def test_all_nodes_accounted_for(self):
        """completed + failed + skipped should equal total_nodes."""
        dag = DAG()
        n1 = dag.add_node(lambda: "ok")
        n2 = dag.add_node(lambda: 1 / 0)
        n3 = dag.add_node(lambda: "child")
        dag.add_edge(n2, n3, continue_on=Condition.Success)
        executor = dag.execute(return_ref=True)

        m = executor.metrics
        assert m.completed_count + m.failed_count + m.skipped_count == m.total_nodes

    def test_completed_set_contains_successful_nodes(self):
        """Nodes that succeed should be in the completed set."""
        dag = DAG()
        n1 = dag.add_node(lambda: 42)
        executor = dag.execute(return_ref=True)

        assert n1 in executor.metrics.completed

    def test_failed_set_contains_failed_nodes(self):
        """Nodes that raise should be in the failed set."""
        dag = DAG()
        n1 = dag.add_node(lambda: 1 / 0)
        executor = dag.execute(return_ref=True)

        assert n1 in executor.metrics.failed

    def test_skipped_set_contains_skipped_nodes(self):
        """Nodes skipped due to parent failure should be in skipped set."""
        dag = DAG()
        parent = dag.add_node(lambda: 1 / 0)
        child = dag.add_node(lambda: "never")
        dag.add_edge(parent, child, continue_on=Condition.Success)
        executor = dag.execute(return_ref=True)

        assert child in executor.metrics.skipped


class TestMetricsTiming:
    """Per-task timing accuracy."""

    def test_task_durations_populated(self):
        """Each executed task should have a duration entry."""
        dag = DAG()
        n1 = dag.add_node(lambda: "fast")
        n2 = dag.add_node(lambda: "also fast")
        executor = dag.execute(return_ref=True)

        assert n1 in executor.metrics.task_durations
        assert n2 in executor.metrics.task_durations

    def test_task_duration_reflects_sleep(self):
        """A task that sleeps should have duration >= sleep time."""
        dag = DAG()
        n1 = dag.add_node(lambda: time.sleep(0.05))
        executor = dag.execute(return_ref=True)

        assert executor.metrics.task_durations[n1] >= 0.04

    def test_parallel_tasks_total_less_than_sum(self):
        """Parallel tasks' total_duration should be less than sum of individual durations."""
        dag = DAG()
        dag.add_node(lambda: time.sleep(0.05))
        dag.add_node(lambda: time.sleep(0.05))
        dag.add_node(lambda: time.sleep(0.05))
        executor = dag.execute(return_ref=True)

        m = executor.metrics
        sum_durations = sum(m.task_durations.values())
        # Total should be much less than sum if truly parallel
        assert m.total_duration < sum_durations * 0.8

    def test_sequential_chain_timing(self):
        """Sequential chain total_duration should be >= sum of task durations."""
        dag = DAG()
        n1 = dag.add_node(lambda: time.sleep(0.03) or "a")
        n2 = dag.add_node(lambda x: time.sleep(0.03) or "b", Promise(n1))
        dag.add_edge(n1, n2)
        executor = dag.execute(return_ref=True)

        m = executor.metrics
        assert m.total_duration >= 0.05  # At least both sleeps

    def test_task_start_times_ordered_for_chain(self):
        """In a sequential chain, second task starts after first."""
        dag = DAG()
        n1 = dag.add_node(lambda: time.sleep(0.02) or "a")
        n2 = dag.add_node(lambda x: "b", Promise(n1))
        dag.add_edge(n1, n2)
        executor = dag.execute(return_ref=True)

        m = executor.metrics
        assert m.task_start_times[n2] > m.task_start_times[n1]


class TestMetricsEdgeCases:
    """Edge cases for metrics collection."""

    def test_empty_dag_metrics(self):
        """Empty DAG should still have a valid metrics object."""
        dag = DAG()
        executor = dag.execute(return_ref=True)

        m = executor.metrics
        assert m.total_duration == 0.0
        assert m.total_nodes == 0
        assert m.completed == set()

    def test_metrics_with_retry(self):
        """Retried tasks should have duration reflecting final successful attempt."""
        attempts = []
        def flaky():
            attempts.append(1)
            if len(attempts) < 2:
                raise ValueError("once")
            return "ok"

        dag = DAG()
        node = dag.add_node(flaky)
        dag.retry_node(node, count=2)
        executor = dag.execute(return_ref=True)

        m = executor.metrics
        assert node in m.completed
        assert node in m.task_durations

    def test_metrics_with_skip(self):
        """Skipped node should not have a meaningful duration but node is in skipped set."""
        dag = DAG()
        parent = dag.add_node(lambda: 1 / 0)
        child = dag.add_node(lambda: "never runs")
        dag.add_edge(parent, child, continue_on=Condition.Success)
        executor = dag.execute(return_ref=True)

        m = executor.metrics
        assert child in m.skipped
        # Skipped nodes were never submitted, so no duration
        assert child not in m.task_durations

    def test_metrics_dataclass_fields(self):
        """DAGMetrics should have all expected fields."""
        dag = DAG()
        dag.add_node(lambda: "x")
        executor = dag.execute(return_ref=True)

        m = executor.metrics
        assert hasattr(m, 'total_duration')
        assert hasattr(m, 'task_durations')
        assert hasattr(m, 'task_start_times')
        assert hasattr(m, 'completed')
        assert hasattr(m, 'failed')
        assert hasattr(m, 'skipped')
        assert hasattr(m, 'total_nodes')
        assert hasattr(m, 'completed_count')
        assert hasattr(m, 'failed_count')
        assert hasattr(m, 'skipped_count')

    def test_metrics_counts_match_sets(self):
        """Count properties should match set lengths."""
        dag = DAG()
        dag.add_node(lambda: "a")
        dag.add_node(lambda: 1 / 0)
        executor = dag.execute(return_ref=True)

        m = executor.metrics
        assert m.completed_count == len(m.completed)
        assert m.failed_count == len(m.failed)
        assert m.skipped_count == len(m.skipped)
