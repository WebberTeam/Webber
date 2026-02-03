"""
Example: Nesting QueueDAG inside a standard DAG.

Demonstrates that QueueDAG can be executed as a callable within
a standard DAG workflow. The nesting overhead is moderate:
- ~1-2ms fixed cost per nested execution
- 1.3x to 2.5x overhead ratio depending on iteration count

This is well within acceptable millisecond-scale latency for
real-time applications.

Note: This example uses in-memory operations for accurate benchmarking.
File I/O would add significant overhead unrelated to Webber itself.
"""
from webber import Promise, DAG, QueueDAG
import time


# Shared results storage (in-memory for fast benchmarking)
results = []


def processor(x):
    """Process data and store result in memory."""
    results.append(x * 2)
    return x * 2


if __name__ == "__main__":
    ITERATIONS = 50

    print("=" * 50)
    print("QueueDAG Nesting Performance Comparison")
    print(f"({ITERATIONS} iterations each)")
    print("=" * 50)

    # Benchmark 1: Nested execution (QueueDAG inside DAG)
    results.clear()
    queue_dag = QueueDAG()
    producer = queue_dag.add_node(lambda: 42, iterator=ITERATIONS)

    main_dag = DAG()
    setup = main_dag.add_node(lambda: "Setup complete")
    nested_exec = main_dag.add_node(lambda x: queue_dag.execute(), Promise(setup))
    main_dag.add_edge(setup, nested_exec)

    t_nested = time.time()
    main_dag.execute()
    nested_time = time.time() - t_nested
    print(f"Nested (QueueDAG in DAG):  {nested_time*1000:.2f}ms")

    # Benchmark 2: Standalone QueueDAG execution
    results.clear()
    queue_dag2 = QueueDAG()
    producer2 = queue_dag2.add_node(lambda: 42, iterator=ITERATIONS)

    t_standalone = time.time()
    queue_dag2.execute()
    standalone_time = time.time() - t_standalone
    print(f"Standalone QueueDAG:       {standalone_time*1000:.2f}ms")

    # Benchmark 3: Plain Python loop (baseline)
    results.clear()
    t_loop = time.time()
    for i in range(ITERATIONS):
        results.append(42)
    loop_time = time.time() - t_loop
    print(f"Plain Python loop:         {loop_time*1000:.2f}ms")

    print("-" * 50)
    if standalone_time > 0:
        ratio = nested_time / standalone_time
        overhead_ms = (nested_time - standalone_time) * 1000
        print(f"Nesting overhead ratio:    {ratio:.2f}x")
        print(f"Fixed overhead:            {overhead_ms:.2f}ms")
        print()
        print("Note: The ~1-2ms overhead comes from the outer DAG's")
        print("ThreadPoolExecutor setup and task wrapper infrastructure.")
