"""
Demonstrates IterPromise: dynamic fan-out from a parent node's list output.

The source node returns a list of cities. The worker node runs once per city
in parallel, and the collector receives the aggregated results as a list.
"""
import webber
from webber import Promise, IterPromise

def get_cities():
    print("Fetching city list...")
    return ["New York", "London", "Tokyo", "Sydney"]

def format_city(city):
    result = f"{city} ({len(city)} chars)"
    print(f"Processing: {city}")
    return result

def summarize(results):
    print(f"\nAll {len(results)} cities processed:")
    for r in results:
        print(f"  - {r}")

if __name__ == "__main__":
    print()

    dag = webber.DAG()

    source = dag.add_node(get_cities)
    worker = dag.add_node(format_city, IterPromise(source))
    collector = dag.add_node(summarize, Promise(worker))

    dag.add_edge(source, worker)
    dag.add_edge(worker, collector)

    dag.execute()
