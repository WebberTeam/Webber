"""
Example: Automatic retry on failure.

Demonstrates how to use DAG.retry_node() to automatically
retry failed operations a specified number of times.
"""
import webber


# Track attempts for demonstration
attempts = [0]


def flaky_api_call():
    """Simulates an unreliable API that fails the first few times."""
    attempts[0] += 1
    print(f"  Attempt {attempts[0]}...")

    if attempts[0] < 3:
        raise ConnectionError(f"API unavailable (attempt {attempts[0]})")

    return f"API response after {attempts[0]} attempts"


def process_response(response):
    """Process the successful API response."""
    print(f"  Processing: {response}")
    return response


if __name__ == "__main__":
    dag = webber.DAG()

    # Add the flaky API call node
    api_node = dag.add_node(flaky_api_call)

    # Configure retry: will retry up to 5 times on failure
    dag.retry_node(api_node, count=5)

    # Add processing node that depends on API result
    process_node = dag.add_node(process_response, webber.Promise(api_node))
    dag.add_edge(api_node, process_node)

    print("Executing DAG with retry logic...")
    print("-" * 40)
    dag.execute()
    print("-" * 40)
    print(f"Completed successfully after {attempts[0]} attempts.")
