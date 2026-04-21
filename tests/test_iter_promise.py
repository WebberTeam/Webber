"""Tests for IterPromise dynamic fan-out."""
import time
import threading
import pytest
import webber
from webber import DAG, Promise, IterPromise, DAGCallbacks
from webber.edges import Condition
from webber.xcoms import InvalidCallable


# --- Module-level named functions ---

def returns_list():
    return [1, 2, 3]

def multiply_by_ten(x):
    return x * 10

def to_upper(s):
    return s.upper()

def identity(x):
    return x


class TestIterPromiseClass:
    """IterPromise construction and type properties."""

    def test_is_promise_subclass(self):
        assert isinstance(IterPromise("x"), Promise)

    def test_creation_with_string(self):
        ip = IterPromise("node_id")
        assert ip.key == "node_id"
        assert ip.keys == ("node_id",)

    def test_creation_with_callable(self):
        ip = IterPromise(returns_list)
        assert ip.key == returns_list
        assert ip.keys == (returns_list,)

    def test_partial_default_false(self):
        assert IterPromise("x").partial is False

    def test_partial_true(self):
        assert IterPromise("x", partial=True).partial is True

    def test_zip_default_false(self):
        assert IterPromise("x").zip is False

    def test_zip_requires_multiple_keys(self):
        ip = IterPromise("a", "b", zip=True)
        assert ip.zip is True
        assert ip.keys == ("a", "b")

    def test_multiple_keys_without_zip_raises(self):
        with pytest.raises(TypeError, match="zip=True"):
            IterPromise("a", "b")

    def test_no_keys_raises(self):
        with pytest.raises(TypeError, match="at least one"):
            IterPromise()

    def test_invalid_key_type_raises(self):
        with pytest.raises(TypeError, match="string IDs or callables"):
            IterPromise(42)

    def test_resolve_preserves_type(self):
        dag = DAG()
        n1 = dag.add_node(returns_list)
        ip = IterPromise(returns_list)
        resolved = dag.resolve_promise(ip)
        assert isinstance(resolved, IterPromise)
        assert not isinstance(resolved, type(None))

    def test_resolve_preserves_partial_flag(self):
        dag = DAG()
        n1 = dag.add_node(returns_list)
        ip = IterPromise(returns_list, partial=True)
        resolved = dag.resolve_promise(ip)
        assert resolved.partial is True

    def test_resolve_preserves_zip_keys(self):
        dag = DAG()
        n1 = dag.add_node(returns_list)
        n2 = dag.add_node(to_upper)
        ip = IterPromise(returns_list, to_upper, zip=True)
        resolved = dag.resolve_promise(ip)
        assert isinstance(resolved, IterPromise)
        assert resolved.zip is True
        assert len(resolved.keys) == 2


class TestBasicFanOut:
    """Core fan-out execution."""

    def test_basic_iter_fanout(self):
        """Producer returns [1,2,3], worker does x*10, downstream gets [10,20,30]."""
        results = {}
        def collector(data):
            results['out'] = data

        dag = DAG()
        source = dag.add_node(lambda: [1, 2, 3])
        worker = dag.add_node(multiply_by_ten, IterPromise(source))
        collect = dag.add_node(collector, Promise(worker))
        dag.add_edge(source, worker)
        dag.add_edge(worker, collect)
        dag.execute()

        assert results['out'] == [10, 20, 30]

    def test_order_preserved(self):
        """Output order should match input order."""
        results = {}
        def collector(data):
            results['out'] = data

        dag = DAG()
        source = dag.add_node(lambda: [5, 3, 1, 4, 2])
        worker = dag.add_node(identity, IterPromise(source))
        collect = dag.add_node(collector, Promise(worker))
        dag.add_edge(source, worker)
        dag.add_edge(worker, collect)
        dag.execute()

        assert results['out'] == [5, 3, 1, 4, 2]

    def test_iter_fanout_with_kwargs(self):
        """IterPromise as kwarg."""
        results = {}
        def worker(item=None):
            return item * 2

        def collector(data):
            results['out'] = data

        dag = DAG()
        source = dag.add_node(lambda: [10, 20, 30])
        w = dag.add_node(worker, item=IterPromise(source))
        c = dag.add_node(collector, Promise(w))
        dag.add_edge(source, w)
        dag.add_edge(w, c)
        dag.execute()

        assert results['out'] == [20, 40, 60]

    def test_single_item_list(self):
        """Parent returns [42] — one sub-task."""
        results = {}
        def collector(data):
            results['out'] = data

        dag = DAG()
        source = dag.add_node(lambda: [42])
        worker = dag.add_node(multiply_by_ten, IterPromise(source))
        collect = dag.add_node(collector, Promise(worker))
        dag.add_edge(source, worker)
        dag.add_edge(worker, collect)
        dag.execute()

        assert results['out'] == [420]

    def test_large_list(self):
        """100 items should all be processed."""
        results = {}
        def collector(data):
            results['out'] = data

        dag = DAG()
        source = dag.add_node(lambda: list(range(100)))
        worker = dag.add_node(identity, IterPromise(source))
        collect = dag.add_node(collector, Promise(worker))
        dag.add_edge(source, worker)
        dag.add_edge(worker, collect)
        dag.execute()

        assert results['out'] == list(range(100))

    def test_string_fanout(self):
        """Fan-out with string items."""
        results = {}
        def collector(data):
            results['out'] = data

        dag = DAG()
        source = dag.add_node(lambda: ["a", "b", "c"])
        worker = dag.add_node(to_upper, IterPromise(source))
        collect = dag.add_node(collector, Promise(worker))
        dag.add_edge(source, worker)
        dag.add_edge(worker, collect)
        dag.execute()

        assert results['out'] == ["A", "B", "C"]


class TestEmptyList:
    """Empty source list edge case."""

    def test_empty_list_completes_immediately(self):
        dag = DAG()
        source = dag.add_node(lambda: [])
        worker = dag.add_node(identity, IterPromise(source))
        dag.add_edge(source, worker)
        executor = dag.execute(return_ref=True)

        assert worker in executor.metrics.completed

    def test_empty_list_downstream_receives_empty(self):
        results = {}
        def collector(data):
            results['out'] = data

        dag = DAG()
        source = dag.add_node(lambda: [])
        worker = dag.add_node(identity, IterPromise(source))
        collect = dag.add_node(collector, Promise(worker))
        dag.add_edge(source, worker)
        dag.add_edge(worker, collect)
        dag.execute()

        assert results['out'] == []


class TestErrorHandling:
    """Fail-fast and partial error modes."""

    def test_subtask_failure_fails_iter_node(self):
        """Default: one sub-task failure → iter-node fails."""
        dag = DAG()
        source = dag.add_node(lambda: [1, 0, 3])
        worker = dag.add_node(lambda x: 10 / x, IterPromise(source))  # 0 causes ZeroDivisionError
        dag.add_edge(source, worker)
        executor = dag.execute(return_ref=True)

        assert worker in executor.metrics.failed

    def test_failure_skips_success_edge(self):
        """Failed iter-node should skip Success-conditioned downstream."""
        results = {}
        def collector(data):
            results['out'] = data

        dag = DAG()
        source = dag.add_node(lambda: [1, 0])
        worker = dag.add_node(lambda x: 10 / x, IterPromise(source))
        collect = dag.add_node(collector, Promise(worker))
        dag.add_edge(source, worker)
        dag.add_edge(worker, collect, continue_on=Condition.Success)
        dag.execute()

        assert 'out' not in results

    def test_failure_runs_failure_edge(self):
        """Failed iter-node should run Failure-conditioned downstream."""
        results = []
        dag = DAG()
        source = dag.add_node(lambda: [1, 0])
        worker = dag.add_node(lambda x: 10 / x, IterPromise(source))
        fallback = dag.add_node(lambda: results.append("fallback"))
        dag.add_edge(source, worker)
        dag.add_edge(worker, fallback, continue_on=Condition.Failure)
        dag.execute()

        assert "fallback" in results

    def test_non_list_parent_raises_type_error(self):
        """Parent returning non-list should raise TypeError."""
        dag = DAG()
        source = dag.add_node(lambda: 42)  # not a list
        worker = dag.add_node(identity, IterPromise(source))
        dag.add_edge(source, worker)

        with pytest.raises(TypeError, match="expected list or tuple"):
            dag.execute()

    def test_partial_mode_collects_successes(self):
        """partial=True: failed sub-tasks become None, iter-node still completes."""
        results = {}
        def collector(data):
            results['out'] = data

        dag = DAG()
        source = dag.add_node(lambda: [1, 0, 3])
        worker = dag.add_node(lambda x: 10 / x, IterPromise(source, partial=True))
        collect = dag.add_node(collector, Promise(worker))
        dag.add_edge(source, worker)
        dag.add_edge(worker, collect, continue_on=Condition.Success)
        dag.execute()

        assert results['out'] == [10.0, None, 10 / 3]

    def test_partial_all_succeed(self):
        """partial=True with no failures should work normally."""
        results = {}
        def collector(data):
            results['out'] = data

        dag = DAG()
        source = dag.add_node(lambda: [1, 2, 3])
        worker = dag.add_node(multiply_by_ten, IterPromise(source, partial=True))
        collect = dag.add_node(collector, Promise(worker))
        dag.add_edge(source, worker)
        dag.add_edge(worker, collect)
        dag.execute()

        assert results['out'] == [10, 20, 30]


class TestZipFanOut:
    """Multi-source zip fan-out."""

    def test_zip_two_sources(self):
        """Zip two parent lists element-wise."""
        results = {}
        def collector(data):
            results['out'] = data

        dag = DAG()
        source_a = dag.add_node(lambda: [1, 2, 3])
        source_b = dag.add_node(lambda: ["a", "b", "c"])
        worker = dag.add_node(lambda pair: f"{pair[0]}{pair[1]}", IterPromise(source_a, source_b, zip=True))
        collect = dag.add_node(collector, Promise(worker))
        dag.add_edge(source_a, worker)
        dag.add_edge(source_b, worker)
        dag.add_edge(worker, collect)
        dag.execute()

        assert results['out'] == ["1a", "2b", "3c"]

    def test_zip_unequal_lengths_truncates(self):
        """Zip with unequal lengths should truncate to shortest."""
        results = {}
        def collector(data):
            results['out'] = data

        dag = DAG()
        source_a = dag.add_node(lambda: [1, 2, 3, 4, 5])
        source_b = dag.add_node(lambda: ["x", "y"])
        worker = dag.add_node(lambda pair: (pair[0], pair[1]), IterPromise(source_a, source_b, zip=True))
        collect = dag.add_node(collector, Promise(worker))
        dag.add_edge(source_a, worker)
        dag.add_edge(source_b, worker)
        dag.add_edge(worker, collect)
        dag.execute()

        assert results['out'] == [(1, "x"), (2, "y")]


class TestMultipleIterPromisesError:
    """Multiple separate IterPromise args on one node."""

    def test_two_iter_promise_args_raises(self):
        """Two separate IterPromise args should raise ValueError."""
        dag = DAG()
        source_a = dag.add_node(lambda: [1, 2])
        source_b = dag.add_node(lambda: [3, 4])
        # Two separate IterPromises (not zip)
        worker = dag.add_node(lambda a, b: a + b, IterPromise(source_a), IterPromise(source_b))
        dag.add_edge(source_a, worker)
        dag.add_edge(source_b, worker)

        with pytest.raises(ValueError, match="IterPromise"):
            dag.execute()


class TestMixedArgs:
    """IterPromise alongside regular Promises and literals."""

    def test_iter_with_literal_and_promise(self):
        """Literal and regular Promise should resolve normally alongside IterPromise."""
        results = {}
        def collector(data):
            results['out'] = data

        def worker(item, multiplier, prefix):
            return f"{prefix}{item * multiplier}"

        dag = DAG()
        config = dag.add_node(lambda: 10)
        source = dag.add_node(lambda: [1, 2, 3])
        w = dag.add_node(worker, IterPromise(source), Promise(config), "v")
        collect = dag.add_node(collector, Promise(w))
        dag.add_edge(config, w)
        dag.add_edge(source, w)
        dag.add_edge(w, collect)
        dag.execute()

        assert results['out'] == ["v10", "v20", "v30"]


class TestCallbacksWithIter:
    """Callbacks fire correctly for iter-nodes."""

    def test_on_task_start_fires_once(self):
        starts = []
        cb = DAGCallbacks(on_task_start=lambda nid, name: starts.append(nid))

        dag = DAG()
        source = dag.add_node(lambda: [1, 2, 3])
        worker = dag.add_node(identity, IterPromise(source))
        dag.add_edge(source, worker)
        dag.execute(callbacks=cb)

        # worker should appear exactly once in starts (not 3 times)
        assert starts.count(worker) == 1

    def test_on_task_complete_receives_aggregated_list(self):
        completed = {}
        cb = DAGCallbacks(on_task_complete=lambda nid, name, result: completed.update({nid: result}))

        dag = DAG()
        source = dag.add_node(lambda: [1, 2, 3])
        worker = dag.add_node(multiply_by_ten, IterPromise(source))
        dag.add_edge(source, worker)
        dag.execute(callbacks=cb)

        assert completed[worker] == [10, 20, 30]

    def test_on_task_error_fires_on_failure(self):
        errors = {}
        cb = DAGCallbacks(on_task_error=lambda nid, name, exc: errors.update({nid: str(exc)}))

        dag = DAG()
        source = dag.add_node(lambda: [1, 0])
        worker = dag.add_node(lambda x: 10 / x, IterPromise(source))
        dag.add_edge(source, worker)
        dag.execute(callbacks=cb)

        assert worker in errors

    def test_on_progress_counts_one_node(self):
        progress = []
        cb = DAGCallbacks(on_progress=lambda c, f, s, t: progress.append((c, f, s, t)))

        dag = DAG()
        source = dag.add_node(lambda: [1, 2, 3])
        worker = dag.add_node(identity, IterPromise(source))
        dag.add_edge(source, worker)
        dag.execute(callbacks=cb)

        # Total should be 2 (source + worker), not 2 + 3 sub-tasks
        assert all(t == 2 for _, _, _, t in progress)


class TestMetricsWithIter:
    """Metrics tracking for iter-nodes."""

    def test_iter_node_in_completed(self):
        dag = DAG()
        source = dag.add_node(lambda: [1, 2, 3])
        worker = dag.add_node(identity, IterPromise(source))
        dag.add_edge(source, worker)
        executor = dag.execute(return_ref=True)

        assert worker in executor.metrics.completed

    def test_total_nodes_unchanged(self):
        dag = DAG()
        source = dag.add_node(lambda: [1, 2, 3])
        worker = dag.add_node(identity, IterPromise(source))
        dag.add_edge(source, worker)
        executor = dag.execute(return_ref=True)

        assert executor.metrics.total_nodes == 2

    def test_task_duration_is_wall_clock(self):
        dag = DAG()
        source = dag.add_node(lambda: [1, 2, 3])
        worker = dag.add_node(lambda x: time.sleep(0.02) or x, IterPromise(source))
        dag.add_edge(source, worker)
        executor = dag.execute(return_ref=True)

        # Wall clock for parallel sub-tasks should be < 3 * 20ms
        assert executor.metrics.task_durations[worker] < 0.15


class TestConcurrency:
    """Sub-tasks run in parallel."""

    def test_subtasks_run_in_parallel(self):
        """N sub-tasks with sleep should complete in ~1x sleep time, not Nx."""
        dag = DAG()
        source = dag.add_node(lambda: list(range(5)))
        worker = dag.add_node(lambda x: time.sleep(0.05) or x, IterPromise(source))
        dag.add_edge(source, worker)

        start = time.time()
        dag.execute()
        duration = time.time() - start

        # 5 tasks * 50ms each sequential = 250ms. Parallel should be ~50ms + overhead.
        assert duration < 0.2


class TestQueueDAGGuard:
    """IterPromise rejected in QueueDAG."""

    def test_queuedag_rejects_iter_promise(self):
        qdag = webber.QueueDAG()
        with pytest.raises(TypeError, match="IterPromise is not supported"):
            qdag.add_node(identity, IterPromise("x"))
