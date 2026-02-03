"""
Example: QueueDAG with halt condition.

Demonstrates how to use halt_condition to stop iterative
execution when a specific condition is met, rather than
running for a fixed number of iterations.
"""
import webber
import time


# Shared state for demonstration
counter = [0]
target = 10


def accumulator():
    """
    Increments counter and returns current value.
    QueueDAG will halt when return value >= target.
    """
    counter[0] += 1
    print(f"  Iteration {counter[0]}: value = {counter[0]}")
    return counter[0]


if __name__ == "__main__":
    qdag = webber.QueueDAG()

    # Add node with halt condition
    # - halt_condition: stops when output >= target
    # - max_iter: safety limit to prevent infinite loops
    node = qdag.add_node(
        accumulator,
        halt_condition=lambda x: x >= target,
        max_iter=1000  # Safety limit
    )

    print(f"Executing QueueDAG until value >= {target}...")
    print("-" * 40)

    start = time.time()
    results = qdag.execute()
    duration = time.time() - start

    print("-" * 40)
    print(f"Stopped at value: {counter[0]}")
    print(f"Results collected: {len(results)} values")
    print(f"Execution time: {duration*1000:.2f}ms")

    # Also demonstrate with a simple iterator for comparison
    print("\n" + "=" * 40)
    print("Comparison: Fixed iterator (no halt condition)")
    print("=" * 40)

    qdag2 = webber.QueueDAG()
    counter2 = [0]

    def simple_counter():
        counter2[0] += 1
        return counter2[0]

    node2 = qdag2.add_node(simple_counter, iterator=10)

    start = time.time()
    results2 = qdag2.execute()
    duration2 = time.time() - start

    print(f"Fixed 10 iterations completed in {duration2*1000:.2f}ms")
    print(f"Results: {results2}")
