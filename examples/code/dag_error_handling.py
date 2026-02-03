"""
Example: Error handling with conditional edges.

Demonstrates how to use Success, Failure, and AnyCase conditions
to create robust workflows with fallback and cleanup logic.
"""
import webber
from webber import Condition, Promise
import random


def risky_operation():
    """Simulates an operation that may fail."""
    if random.random() < 0.5:
        raise ValueError("Operation failed!")
    return "Operation succeeded"


def handle_success(result):
    """Called only when risky_operation succeeds."""
    print(f"Success handler: {result}")
    return result


def handle_failure():
    """Called only when risky_operation fails."""
    print("Fallback handler: Executing recovery logic...")
    return "Recovered"


def cleanup():
    """Called regardless of success or failure."""
    print("Cleanup: Releasing resources...")


if __name__ == "__main__":
    dag = webber.DAG()

    # Add nodes
    risky = dag.add_node(risky_operation)
    success = dag.add_node(handle_success, Promise(risky))
    fallback = dag.add_node(handle_failure)
    cleanup_node = dag.add_node(cleanup)

    # Configure conditional edges
    dag.add_edge(risky, success, continue_on=Condition.Success)
    dag.add_edge(risky, fallback, continue_on=Condition.Failure)
    dag.add_edge(risky, cleanup_node, continue_on=Condition.AnyCase)

    print("Executing DAG with conditional edges...")
    print("-" * 40)
    dag.execute()
    print("-" * 40)
    print("DAG execution complete.")
