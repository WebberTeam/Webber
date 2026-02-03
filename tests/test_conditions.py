"""
Conditional Execution Tests - Priority P0

Tests for Success, Failure, and AnyCase condition types in DAG execution.
"""
import pytest
import webber
from webber import Condition, Promise


class TestSuccessCondition:
    """Tests for Condition.Success execution logic."""

    def test_child_executes_on_parent_success(self, empty_dag):
        """Child node should execute when parent succeeds."""
        results = []
        node1 = empty_dag.add_node(lambda: results.append("parent") or "success")
        node2 = empty_dag.add_node(lambda: results.append("child"))

        empty_dag.add_edge(node1, node2, continue_on=Condition.Success)
        empty_dag.execute()

        assert results == ["parent", "child"]

    def test_child_skipped_on_parent_failure(self, empty_dag):
        """Child node should NOT execute when parent fails (Success condition)."""
        results = []
        node1 = empty_dag.add_node(lambda: 1/0)  # Will fail
        node2 = empty_dag.add_node(lambda: results.append("child"))

        empty_dag.add_edge(node1, node2, continue_on=Condition.Success)
        empty_dag.execute()

        assert "child" not in results  # Child should be skipped

    def test_success_condition_default(self, empty_dag):
        """Success should be the default condition."""
        results = []
        node1 = empty_dag.add_node(lambda: results.append("parent") or "success")
        node2 = empty_dag.add_node(lambda: results.append("child"))

        # Not specifying continue_on - should default to Success
        empty_dag.add_edge(node1, node2)
        empty_dag.execute()

        assert results == ["parent", "child"]

    def test_success_condition_with_promise(self, empty_dag):
        """Success condition with Promise should pass parent result."""
        results = []
        node1 = empty_dag.add_node(lambda: 42)
        node2 = empty_dag.add_node(
            lambda x: results.append(x),
            Promise(node1)
        )

        empty_dag.add_edge(node1, node2, continue_on=Condition.Success)
        empty_dag.execute()

        assert results == [42]

    def test_multiple_children_success_condition(self, empty_dag):
        """Multiple children should all execute on parent success."""
        results = []
        parent = empty_dag.add_node(lambda: results.append("parent") or "success")
        child1 = empty_dag.add_node(lambda: results.append("child1"))
        child2 = empty_dag.add_node(lambda: results.append("child2"))

        empty_dag.add_edge(parent, child1, continue_on=Condition.Success)
        empty_dag.add_edge(parent, child2, continue_on=Condition.Success)
        empty_dag.execute()

        assert "parent" in results
        assert "child1" in results
        assert "child2" in results


class TestFailureCondition:
    """Tests for Condition.Failure execution logic."""

    def test_child_executes_on_parent_failure(self, empty_dag):
        """Child node should execute when parent fails (Failure condition)."""
        results = []
        node1 = empty_dag.add_node(lambda: 1/0)  # Will fail
        node2 = empty_dag.add_node(lambda: results.append("fallback"))

        empty_dag.add_edge(node1, node2, continue_on=Condition.Failure)
        empty_dag.execute()

        assert "fallback" in results

    def test_child_skipped_on_parent_success(self, empty_dag):
        """Child node should NOT execute when parent succeeds (Failure condition)."""
        results = []
        node1 = empty_dag.add_node(lambda: "success")
        node2 = empty_dag.add_node(lambda: results.append("fallback"))

        empty_dag.add_edge(node1, node2, continue_on=Condition.Failure)
        empty_dag.execute()

        assert "fallback" not in results

    def test_failure_condition_error_handling(self, empty_dag):
        """Failure condition should catch various error types."""
        results = []

        # Division by zero
        node1 = empty_dag.add_node(lambda: 1/0)
        fallback1 = empty_dag.add_node(lambda: results.append("div_zero_caught"))

        # Key error
        node2 = empty_dag.add_node(lambda: {}['missing_key'])
        fallback2 = empty_dag.add_node(lambda: results.append("key_error_caught"))

        empty_dag.add_edge(node1, fallback1, continue_on=Condition.Failure)
        empty_dag.add_edge(node2, fallback2, continue_on=Condition.Failure)
        empty_dag.execute()

        assert "div_zero_caught" in results
        assert "key_error_caught" in results

    def test_failure_condition_no_promise_value(self, empty_dag):
        """Failure condition child cannot use Promise to failed parent."""
        results = []
        node1 = empty_dag.add_node(lambda: 1/0)  # Will fail
        # Note: Promise would try to get result from failed node
        node2 = empty_dag.add_node(lambda: results.append("fallback"))

        empty_dag.add_edge(node1, node2, continue_on=Condition.Failure)
        empty_dag.execute()

        assert "fallback" in results


class TestAnyCaseCondition:
    """Tests for Condition.AnyCase execution logic."""

    def test_anycase_on_success(self, empty_dag):
        """Child should execute when parent succeeds (AnyCase)."""
        results = []
        node1 = empty_dag.add_node(lambda: "success")
        node2 = empty_dag.add_node(lambda: results.append("cleanup"))

        empty_dag.add_edge(node1, node2, continue_on=Condition.AnyCase)
        empty_dag.execute()

        assert "cleanup" in results

    def test_anycase_on_failure(self, empty_dag):
        """Child should execute when parent fails (AnyCase)."""
        results = []
        node1 = empty_dag.add_node(lambda: 1/0)  # Will fail
        node2 = empty_dag.add_node(lambda: results.append("cleanup"))

        empty_dag.add_edge(node1, node2, continue_on=Condition.AnyCase)
        empty_dag.execute()

        assert "cleanup" in results

    def test_anycase_cleanup_pattern(self, empty_dag):
        """AnyCase is useful for cleanup/finally patterns."""
        results = []
        risky = empty_dag.add_node(lambda: results.append("risky") or (1/0))
        cleanup = empty_dag.add_node(lambda: results.append("cleanup"))
        final = empty_dag.add_node(lambda: results.append("final"))

        empty_dag.add_edge(risky, cleanup, continue_on=Condition.AnyCase)
        empty_dag.add_edge(cleanup, final, continue_on=Condition.Success)
        empty_dag.execute()

        assert "risky" in results
        assert "cleanup" in results  # Runs despite risky failing
        assert "final" in results     # Runs because cleanup succeeded


class TestConditionalBranching:
    """Tests for complex conditional scenarios."""

    def test_conditional_branching(self, empty_dag):
        """DAG should branch based on parent success/failure."""
        results = []
        risky = empty_dag.add_node(lambda: 1/0)  # Will fail
        success_path = empty_dag.add_node(lambda: results.append("success"))
        failure_path = empty_dag.add_node(lambda: results.append("failure"))

        empty_dag.add_edge(risky, success_path, continue_on=Condition.Success)
        empty_dag.add_edge(risky, failure_path, continue_on=Condition.Failure)
        empty_dag.execute()

        assert results == ["failure"]

    def test_multiple_parents_different_conditions(self, empty_dag):
        """Node with multiple parents should wait for all conditions."""
        results = []
        parent1 = empty_dag.add_node(lambda: "success")
        parent2 = empty_dag.add_node(lambda: 1/0)  # Will fail
        child = empty_dag.add_node(lambda: results.append("executed"))

        empty_dag.add_edge(parent1, child, continue_on=Condition.Success)
        empty_dag.add_edge(parent2, child, continue_on=Condition.Failure)
        empty_dag.execute()

        # Child should execute only when BOTH conditions are met
        assert "executed" in results

    def test_diamond_with_mixed_conditions(self, empty_dag):
        """Diamond DAG with different conditions on different paths."""
        results = []
        root = empty_dag.add_node(lambda: results.append("root") or "value")
        left = empty_dag.add_node(lambda: results.append("left"))
        right = empty_dag.add_node(lambda: results.append("right"))
        merge = empty_dag.add_node(lambda: results.append("merge"))

        empty_dag.add_edge(root, left, continue_on=Condition.Success)
        empty_dag.add_edge(root, right, continue_on=Condition.Success)
        empty_dag.add_edge(left, merge, continue_on=Condition.Success)
        empty_dag.add_edge(right, merge, continue_on=Condition.Success)
        empty_dag.execute()

        assert all(x in results for x in ["root", "left", "right", "merge"])

    def test_cascading_failures(self, empty_dag):
        """Failures should cascade through Success-conditioned edges."""
        results = []
        node1 = empty_dag.add_node(lambda: results.append("1") or (1/0))
        node2 = empty_dag.add_node(lambda: results.append("2"))
        node3 = empty_dag.add_node(lambda: results.append("3"))

        empty_dag.add_edge(node1, node2, continue_on=Condition.Success)
        empty_dag.add_edge(node2, node3, continue_on=Condition.Success)
        empty_dag.execute()

        # Only node1 should execute
        assert results == ["1"]

    def test_recovery_after_failure(self, empty_dag):
        """Failure path can lead to recovery and continued execution."""
        results = []
        risky = empty_dag.add_node(lambda: 1/0)
        recover = empty_dag.add_node(lambda: results.append("recover") or "recovered")
        continue_on = empty_dag.add_node(lambda: results.append("continue"))

        empty_dag.add_edge(risky, recover, continue_on=Condition.Failure)
        empty_dag.add_edge(recover, continue_on, continue_on=Condition.Success)
        empty_dag.execute()

        assert results == ["recover", "continue"]


class TestConditionValidation:
    """Tests for condition validation and error handling."""

    def test_invalid_condition_type(self, empty_dag):
        """Invalid Condition value should raise error."""
        node1 = empty_dag.add_node(lambda: "test")
        node2 = empty_dag.add_node(lambda: "test2")

        with pytest.raises((ValueError, TypeError)):
            empty_dag.add_edge(node1, node2, continue_on="invalid")

    def test_invalid_condition_integer(self, empty_dag):
        """Integer that's not a valid Condition should raise error."""
        node1 = empty_dag.add_node(lambda: "test")
        node2 = empty_dag.add_node(lambda: "test2")

        with pytest.raises((ValueError, TypeError)):
            empty_dag.add_edge(node1, node2, continue_on=999)

    def test_update_edge_condition(self, empty_dag):
        """Updating edge condition should work."""
        node1 = empty_dag.add_node(lambda: "test")
        node2 = empty_dag.add_node(lambda: "test2")
        empty_dag.add_edge(node1, node2, continue_on=Condition.Success)

        # Update condition
        empty_dag.update_edges((node1, node2), continue_on=Condition.AnyCase)

        edge = empty_dag.get_edge(node1, node2)
        assert edge['Condition'] == Condition.AnyCase

    def test_get_edge_condition(self, empty_dag):
        """Should be able to retrieve edge condition."""
        node1 = empty_dag.add_node(lambda: "test")
        node2 = empty_dag.add_node(lambda: "test2")
        empty_dag.add_edge(node1, node2, continue_on=Condition.Failure)

        edge = empty_dag.get_edge(node1, node2)
        assert edge['Condition'] == Condition.Failure


class TestConditionEnumValues:
    """Tests for Condition enum values and properties."""

    def test_condition_values(self):
        """Condition enum should have expected values."""
        assert Condition.Success == 0
        assert Condition.Failure == 1
        assert Condition.AnyCase == 3

    def test_condition_is_int_enum(self):
        """Conditions should be comparable as integers."""
        assert isinstance(Condition.Success.value, int)
        assert isinstance(Condition.Failure.value, int)
        assert isinstance(Condition.AnyCase.value, int)

    def test_condition_in_check(self):
        """Should be able to check if value is in Condition."""
        assert Condition.Success in Condition
        assert Condition.Failure in Condition
        assert Condition.AnyCase in Condition
        assert 999 not in Condition
