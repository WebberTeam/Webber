"""
Demonstrates DAGMetrics: structured execution telemetry from completed runs.

Execute with return_ref=True, then inspect executor.metrics for per-task
timing, completion tracking, and summary statistics.
"""
import time
import webber
from webber import Promise

def slow_extract():
    """Simulates a slow data source."""
    time.sleep(0.1)
    return {"items": 1000}

def fast_validate(data):
    """Quick validation step."""
    assert data["items"] > 0
    return data

def slow_transform(data):
    """Heavy transformation."""
    time.sleep(0.08)
    return {**data, "transformed": True}

def fast_load(data):
    """Quick load step."""
    return f"Loaded {data['items']} items"

if __name__ == "__main__":
    print()

    dag = webber.DAG()

    e = dag.add_node(slow_extract)
    dag.relabel_node(e, "extract")
    v = dag.add_node(fast_validate, Promise(e))
    dag.relabel_node(v, "validate")
    t = dag.add_node(slow_transform, Promise(v))
    dag.relabel_node(t, "transform")
    l = dag.add_node(fast_load, Promise(t))
    dag.relabel_node(l, "load")

    dag.add_edge(e, v)
    dag.add_edge(v, t)
    dag.add_edge(t, l)

    executor = dag.execute(return_ref=True)
    m = executor.metrics

    print("Execution Metrics")
    print("=" * 40)
    print(f"Total duration:  {m.total_duration*1000:.1f}ms")
    print(f"Nodes completed: {m.completed_count}")
    print(f"Nodes failed:    {m.failed_count}")
    print(f"Nodes skipped:   {m.skipped_count}")
    print()

    print("Per-Task Timing")
    print("-" * 40)
    for node_id, duration in sorted(m.task_durations.items(), key=lambda x: x[1], reverse=True):
        name = dag.graph.nodes[node_id].get("name", "?")
        pct = (duration / m.total_duration) * 100
        bar = "#" * int(pct / 5)
        print(f"  {name:12s}  {duration*1000:6.1f}ms  {pct:4.1f}%  {bar}")
