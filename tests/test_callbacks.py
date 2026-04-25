"""Tests for DAGCallbacks progress observation system."""
import time
import threading
import pytest
import webber
from webber import DAG, DAGCallbacks, Promise
from webber.edges import Condition


class TestCallbackFiring:
    """Verify that all callback types fire at the correct times."""

    def test_on_dag_start_fires(self):
        """on_dag_start should fire with total node count before execution."""
        received = []
        cb = DAGCallbacks(on_dag_start=lambda total: received.append(total))

        dag = DAG()
        dag.add_node(lambda: "a")
        dag.add_node(lambda: "b")
        dag.execute(callbacks=cb)

        assert received == [2]

    def test_on_dag_complete_fires(self):
        """on_dag_complete should fire with final counts and duration."""
        received = []
        cb = DAGCallbacks(
            on_dag_complete=lambda c, f, s, d: received.append((c, f, s, d > 0))
        )

        dag = DAG()
        dag.add_node(lambda: "done")
        dag.execute(callbacks=cb)

        assert len(received) == 1
        assert received[0] == (1, 0, 0, True)

    def test_on_task_start_fires_for_each_node(self):
        """on_task_start should fire once per submitted node."""
        started = []
        cb = DAGCallbacks(on_task_start=lambda nid, name: started.append(name))

        dag = DAG()
        dag.add_node(lambda: "x")
        dag.add_node(lambda: "y")
        dag.execute(callbacks=cb)

        assert len(started) == 2
        assert "<lambda>" in started[0]

    def test_on_task_complete_fires_on_success(self):
        """on_task_complete should fire with node_id, name, and result."""
        completed = []
        cb = DAGCallbacks(
            on_task_complete=lambda nid, name, result: completed.append(result)
        )

        dag = DAG()
        dag.add_node(lambda: 42)
        dag.execute(callbacks=cb)

        assert completed == [42]

    def test_on_task_error_fires_on_failure(self):
        """on_task_error should fire with node_id, name, and exception."""
        errors = []
        cb = DAGCallbacks(
            on_task_error=lambda nid, name, exc: errors.append(str(exc))
        )

        dag = DAG()
        dag.add_node(lambda: 1 / 0)
        dag.execute(callbacks=cb)

        assert len(errors) == 1
        assert "division by zero" in errors[0]

    def test_on_progress_fires_after_each_state_change(self):
        """on_progress should fire after each node completes/fails/skips."""
        progress = []
        cb = DAGCallbacks(
            on_progress=lambda c, f, s, t: progress.append((c, f, s, t))
        )

        dag = DAG()
        n1 = dag.add_node(lambda: "first")
        n2 = dag.add_node(lambda: "second")
        dag.add_edge(n1, n2)
        dag.execute(callbacks=cb)

        assert len(progress) == 2
        # First progress: 1 complete, total 2
        assert progress[0] == (1, 0, 0, 2)
        # Second progress: 2 complete, total 2
        assert progress[1] == (2, 0, 0, 2)


class TestCallbackWithConditions:
    """Test callbacks interact correctly with conditional execution."""

    def test_error_callback_with_failure_edge(self):
        """Failed node triggers on_task_error, then fallback triggers on_task_complete."""
        events = []
        cb = DAGCallbacks(
            on_task_error=lambda nid, name, exc: events.append(("error", name)),
            on_task_complete=lambda nid, name, r: events.append(("complete", name)),
        )

        dag = DAG()
        risky = dag.add_node(lambda: 1 / 0)
        dag.relabel_node(risky, "risky")
        fallback = dag.add_node(lambda: "recovered")
        dag.relabel_node(fallback, "fallback")
        dag.add_edge(risky, fallback, continue_on=Condition.Failure)
        dag.execute(callbacks=cb)

        assert ("error", "risky") in events
        assert ("complete", "fallback") in events

    def test_skipped_nodes_counted_in_progress(self):
        """Skipped nodes should appear in the progress skipped count."""
        progress = []
        cb = DAGCallbacks(
            on_progress=lambda c, f, s, t: progress.append((c, f, s, t))
        )

        dag = DAG()
        parent = dag.add_node(lambda: 1 / 0)
        child = dag.add_node(lambda: "should skip")
        dag.add_edge(parent, child, continue_on=Condition.Success)
        dag.execute(callbacks=cb)

        # Parent fails → child gets skipped
        last = progress[-1]
        assert last[1] == 1  # 1 failed
        assert last[2] == 1  # 1 skipped


class TestCallbackEdgeCases:
    """Edge cases and safety of the callback system."""

    def test_no_callbacks_zero_overhead(self):
        """Execution without callbacks should work identically to before."""
        dag = DAG()
        n1 = dag.add_node(lambda: 42)
        n2 = dag.add_node(lambda x: x * 2, Promise(n1))
        dag.add_edge(n1, n2)
        dag.execute()  # No callbacks, no crash

    def test_none_callbacks_accepted(self):
        """Passing callbacks=None should be equivalent to no callbacks."""
        dag = DAG()
        dag.add_node(lambda: "ok")
        dag.execute(callbacks=None)

    def test_partial_callbacks(self):
        """Only some callback fields set — others should not fire."""
        received = []
        cb = DAGCallbacks(on_dag_start=lambda t: received.append(t))
        # on_task_complete etc. are all None

        dag = DAG()
        dag.add_node(lambda: "test")
        dag.execute(callbacks=cb)

        assert received == [1]

    def test_empty_dag_no_callbacks_fired(self):
        """Empty DAG should not fire any callbacks except possibly dag_start with 0."""
        received = []
        cb = DAGCallbacks(
            on_dag_start=lambda t: received.append(("start", t)),
            on_dag_complete=lambda c, f, s, d: received.append(("complete",)),
        )

        dag = DAG()
        dag.execute(callbacks=cb)

        # Empty DAG returns early, no callbacks should fire
        assert received == []

    def test_callbacks_with_retry(self):
        """Retried nodes should fire on_task_start again on retry."""
        starts = []
        cb = DAGCallbacks(on_task_start=lambda nid, name: starts.append(name))

        attempts = []
        def flaky():
            attempts.append(1)
            if len(attempts) < 2:
                raise ValueError("fail once")
            return "ok"

        dag = DAG()
        node = dag.add_node(flaky)
        dag.retry_node(node, count=2)
        dag.execute(callbacks=cb)

        # Should have been started twice (initial + 1 retry)
        assert len(starts) == 2

    def test_callback_receives_correct_result(self):
        """on_task_complete should receive the actual return value."""
        results = {}
        cb = DAGCallbacks(
            on_task_complete=lambda nid, name, result: results.update({name: result})
        )

        dag = DAG()
        dag.add_node(lambda: {"key": "value"})
        dag.execute(callbacks=cb)

        assert results["<lambda>"] == {"key": "value"}

    def test_dag_complete_duration_reasonable(self):
        """on_dag_complete duration should reflect actual execution time."""
        duration = []
        cb = DAGCallbacks(
            on_dag_complete=lambda c, f, s, d: duration.append(d)
        )

        dag = DAG()
        dag.add_node(lambda: time.sleep(0.05))
        dag.execute(callbacks=cb)

        assert len(duration) == 1
        assert duration[0] >= 0.04  # At least ~50ms
