"""
Demonstrates IterPromise partial mode: resilient fan-out that collects
successes and substitutes None for failures instead of failing the whole node.

Useful when some items in a batch may fail but you still want partial results.
"""
import webber
from webber import Promise, IterPromise

def get_urls():
    return [
        "https://example.com/api/1",
        "https://example.com/api/INVALID",  # will fail
        "https://example.com/api/3",
        "https://example.com/api/4",
    ]

def fetch_data(url):
    """Simulate fetching — fails for URLs containing 'INVALID'."""
    if "INVALID" in url:
        raise ConnectionError(f"Failed to fetch: {url}")
    result = f"data_from_{url.split('/')[-1]}"
    print(f"  Fetched: {result}")
    return result

def process_results(results):
    succeeded = [r for r in results if r is not None]
    failed_count = results.count(None)
    print(f"\nResults: {len(succeeded)} succeeded, {failed_count} failed")
    for r in results:
        print(f"  {'[OK] ' + r if r else '[FAILED] None'}")

if __name__ == "__main__":
    print()

    dag = webber.DAG()

    urls = dag.add_node(get_urls)
    # partial=True: failed sub-tasks become None instead of failing the whole node
    fetcher = dag.add_node(fetch_data, IterPromise(urls, partial=True))
    processor = dag.add_node(process_results, Promise(fetcher))

    dag.add_edge(urls, fetcher)
    dag.add_edge(fetcher, processor)

    dag.execute()
