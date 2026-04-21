"""Smoke tests for the execution dashboard visualization."""
import time
import pytest
import webber
from webber import DAG, Promise, DAGCallbacks
from webber.edges import Condition


class TestDashboardIntegration:
    """Test that dashboard components wire together correctly."""

    def test_executor_has_dashboard_method(self):
        """DAGExecutor should expose a dashboard() method."""
        dag = DAG()
        dag.add_node(lambda: "test")
        executor = dag.execute(return_ref=True)

        assert hasattr(executor, 'dashboard')
        assert callable(executor.dashboard)

    def test_metrics_available_for_dashboard(self):
        """Executor metrics should contain all data needed by dashboard."""
        dag = DAG()
        n1 = dag.add_node(lambda: "a")
        n2 = dag.add_node(lambda x: "b", Promise(n1))
        dag.add_edge(n1, n2)
        executor = dag.execute(return_ref=True)

        m = executor.metrics
        # Dashboard needs these fields
        assert hasattr(m, 'total_duration')
        assert hasattr(m, 'completed')
        assert hasattr(m, 'failed')
        assert hasattr(m, 'skipped')
        assert hasattr(m, 'task_durations')
        assert hasattr(m, 'total_nodes')
        assert m.total_nodes == 2
        assert m.completed_count == 2

    def test_dashboard_with_mixed_results(self):
        """Metrics should correctly categorize nodes by status for dashboard coloring."""
        dag = DAG()
        ok = dag.add_node(lambda: "success")
        fail = dag.add_node(lambda: 1 / 0)
        child = dag.add_node(lambda: "skipped")
        dag.add_edge(fail, child, continue_on=Condition.Success)
        executor = dag.execute(return_ref=True)

        m = executor.metrics
        assert ok in m.completed
        assert fail in m.failed
        assert child in m.skipped

    def test_task_durations_for_dashboard_list(self):
        """Task durations should be sortable for the dashboard sidebar list."""
        dag = DAG()
        dag.add_node(lambda: time.sleep(0.02) or "slow")
        dag.add_node(lambda: "fast")
        executor = dag.execute(return_ref=True)

        m = executor.metrics
        # Should be able to sort by duration (descending)
        sorted_tasks = sorted(m.task_durations.items(), key=lambda x: x[1], reverse=True)
        assert len(sorted_tasks) == 2
        # First item should be the slower one
        assert sorted_tasks[0][1] >= sorted_tasks[1][1]


class TestDashboardVizFunction:
    """Test the visualize_dashboard function (without actually serving)."""

    def test_viz_module_has_dashboard_function(self):
        """webber.viz should expose visualize_dashboard."""
        import webber.viz as viz
        assert hasattr(viz, 'visualize_dashboard')
        assert callable(viz.visualize_dashboard)

    def test_dashboard_rejects_empty_graph(self):
        """Dashboard should raise RuntimeError for empty DAG."""
        import webber.viz as viz
        import networkx as nx
        from webber.core import DAGMetrics

        empty_graph = nx.DiGraph()
        empty_metrics = DAGMetrics(total_duration=0.0, total_nodes=0)

        with pytest.raises(RuntimeError, match="empty DAG"):
            viz.visualize_dashboard(empty_graph, empty_metrics)

    def test_end_to_end_callbacks_and_metrics_together(self):
        """Full integration: callbacks + metrics + dashboard-ready state."""
        progress_log = []
        cb = DAGCallbacks(
            on_progress=lambda c, f, s, t: progress_log.append((c, f, s, t))
        )

        dag = DAG()
        n1 = dag.add_node(lambda: 42)
        n2 = dag.add_node(lambda x: x * 2, Promise(n1))
        dag.add_edge(n1, n2)

        executor = dag.execute(return_ref=True, callbacks=cb)

        # Callbacks fired
        assert len(progress_log) == 2

        # Metrics populated
        m = executor.metrics
        assert m.completed_count == 2
        assert m.total_duration > 0
        assert n1 in m.task_durations
        assert n2 in m.task_durations
