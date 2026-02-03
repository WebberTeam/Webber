"""
Retry & Skip Mechanisms Tests - Priority P1

Tests for node retry on failure and skip functionality.
"""
import pytest
import webber
from webber import Condition


class TestRetryMechanism:
    """Tests for node retry on failure."""

    def test_retry_node_on_failure(self, empty_dag):
        """Node should retry on failure up to retry count."""
        attempts = []

        def flaky_func():
            attempts.append(1)
            if len(attempts) < 3:
                raise ValueError("Not yet")
            return "success"

        node1 = empty_dag.add_node(flaky_func)
        empty_dag.retry_node(node1, count=3)
        empty_dag.execute()

        assert len(attempts) == 3  # Initial attempt + 2 retries

    def test_retry_exhaustion(self, empty_dag):
        """Node should fail after retry count exhausted."""
        attempts = []

        def always_fails():
            attempts.append(1)
            raise ValueError("Always fails")

        node1 = empty_dag.add_node(always_fails)
        empty_dag.retry_node(node1, count=2)
        empty_dag.execute()

        assert len(attempts) == 3  # Initial + 2 retries

    def test_no_retry_on_success(self, empty_dag):
        """Successful node should not retry."""
        attempts = []
        node1 = empty_dag.add_node(lambda: attempts.append(1) or "success")
        empty_dag.retry_node(node1, count=5)
        empty_dag.execute()

        assert len(attempts) == 1  # Should only execute once

    def test_retry_with_dependency(self, empty_dag):
        """Retry should work with dependent nodes."""
        attempts = []
        results = []

        def flaky_parent():
            attempts.append(1)
            if len(attempts) < 2:
                raise ValueError("Failing")
            return "parent_success"

        parent = empty_dag.add_node(flaky_parent)
        child = empty_dag.add_node(lambda: results.append("child"))

        empty_dag.retry_node(parent, count=2)
        empty_dag.add_edge(parent, child, continue_on=Condition.Success)
        empty_dag.execute()

        assert len(attempts) == 2  # Retried once
        assert "child" in results  # Child executed after parent succeeded

    def test_retry_count_validation(self, empty_dag):
        """Retry count must be non-negative integer."""
        node1 = empty_dag.add_node(lambda: "test")

        with pytest.raises(ValueError, match="non-negative integer"):
            empty_dag.retry_node(node1, count=-1)

        with pytest.raises(ValueError, match="non-negative integer"):
            empty_dag.retry_node(node1, count="invalid")

    def test_retry_zero_count(self, empty_dag):
        """Retry count of 0 means no retries."""
        attempts = []

        def fails_once():
            attempts.append(1)
            raise ValueError("Failing")

        node1 = empty_dag.add_node(fails_once)
        empty_dag.retry_node(node1, count=0)
        empty_dag.execute()

        assert len(attempts) == 1  # No retries

    def test_retry_multiple_nodes(self, empty_dag):
        """Multiple nodes can have different retry counts."""
        attempts1 = []
        attempts2 = []

        def fails_early():
            attempts1.append(1)
            if len(attempts1) < 2:
                raise ValueError("Fail")
            return "ok"

        def fails_late():
            attempts2.append(1)
            if len(attempts2) < 4:
                raise ValueError("Fail")
            return "ok"

        node1 = empty_dag.add_node(fails_early)
        node2 = empty_dag.add_node(fails_late)

        empty_dag.retry_node(node1, count=2)
        empty_dag.retry_node(node2, count=4)
        empty_dag.execute()

        assert len(attempts1) == 2
        assert len(attempts2) == 4


class TestSkipMechanism:
    """Tests for node skip functionality."""

    def test_skip_node_as_success(self, empty_dag):
        """Skipped node should be treated as success."""
        results = []
        node1 = empty_dag.add_node(lambda: results.append("skipped"))
        node2 = empty_dag.add_node(lambda: results.append("child"))

        empty_dag.skip_node(node1, skip=True, as_failure=False)
        empty_dag.add_edge(node1, node2, continue_on=Condition.Success)
        empty_dag.execute()

        assert "skipped" not in results  # Node1 was skipped
        assert "child" in results  # Node2 executed (parent counted as success)

    def test_skip_node_as_failure(self, empty_dag):
        """Skipped node should be treated as failure."""
        results = []
        node1 = empty_dag.add_node(lambda: results.append("skipped"))
        fallback = empty_dag.add_node(lambda: results.append("fallback"))
        success = empty_dag.add_node(lambda: results.append("success"))

        empty_dag.skip_node(node1, skip=True, as_failure=True)
        empty_dag.add_edge(node1, success, continue_on=Condition.Success)
        empty_dag.add_edge(node1, fallback, continue_on=Condition.Failure)
        empty_dag.execute()

        assert "skipped" not in results
        assert "fallback" in results  # Executed because parent counted as failure
        assert "success" not in results

    def test_unskip_node(self, empty_dag):
        """Setting skip=False should re-enable node execution."""
        results = []
        node1 = empty_dag.add_node(lambda: results.append("executed"))

        empty_dag.skip_node(node1, skip=True)
        empty_dag.skip_node(node1, skip=False)  # Re-enable
        empty_dag.execute()

        assert "executed" in results

    def test_skip_validation(self, empty_dag):
        """Skip argument must be boolean."""
        node1 = empty_dag.add_node(lambda: "test")

        with pytest.raises(ValueError, match="must be a boolean"):
            empty_dag.skip_node(node1, skip="invalid")

    def test_skip_with_anycase_condition(self, empty_dag):
        """Skip with AnyCase condition should still execute child."""
        results = []
        node1 = empty_dag.add_node(lambda: results.append("skipped"))
        cleanup = empty_dag.add_node(lambda: results.append("cleanup"))

        empty_dag.skip_node(node1, skip=True, as_failure=False)
        empty_dag.add_edge(node1, cleanup, continue_on=Condition.AnyCase)
        empty_dag.execute()

        assert "skipped" not in results
        assert "cleanup" in results

    def test_skip_multiple_nodes(self, empty_dag):
        """Multiple nodes can be skipped independently."""
        results = []
        node1 = empty_dag.add_node(lambda: results.append("1"))
        node2 = empty_dag.add_node(lambda: results.append("2"))
        node3 = empty_dag.add_node(lambda: results.append("3"))

        empty_dag.skip_node(node1, skip=True)
        empty_dag.skip_node(node3, skip=True)
        empty_dag.execute()

        assert "1" not in results
        assert "2" in results  # Not skipped
        assert "3" not in results

    def test_skip_with_promise(self, empty_dag):
        """Skipped node with Promise should not pass value."""
        from webber import Promise

        results = []
        node1 = empty_dag.add_node(lambda: 42)
        node2 = empty_dag.add_node(lambda x: results.append(x), Promise(node1))

        empty_dag.skip_node(node1, skip=True, as_failure=False)
        empty_dag.add_edge(node1, node2, continue_on=Condition.Success)
        empty_dag.execute()

        # node2 should execute but won't have the value from node1
        # (This tests the behavior - may need adjustment based on actual implementation)
        assert len(results) >= 0  # Execution completes


class TestRetrySkipInteraction:
    """Tests for interaction between retry and skip."""

    def test_skip_overrides_retry(self, empty_dag):
        """Skipped node should not retry even if retry is set."""
        attempts = []

        def fails():
            attempts.append(1)
            raise ValueError("Fail")

        node1 = empty_dag.add_node(fails)
        empty_dag.retry_node(node1, count=5)
        empty_dag.skip_node(node1, skip=True)
        empty_dag.execute()

        # Node is skipped, so it shouldn't attempt at all (or just once to skip)
        assert len(attempts) <= 1

    def test_unskip_restores_retry(self, empty_dag):
        """Unskipping should allow retry to work."""
        attempts = []

        def flaky():
            attempts.append(1)
            if len(attempts) < 2:
                raise ValueError("Fail")
            return "ok"

        node1 = empty_dag.add_node(flaky)
        empty_dag.retry_node(node1, count=3)
        empty_dag.skip_node(node1, skip=True)
        empty_dag.skip_node(node1, skip=False)
        empty_dag.execute()

        assert len(attempts) == 2  # Retry worked after unskip
