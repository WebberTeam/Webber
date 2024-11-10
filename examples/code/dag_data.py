"""
Simple demo of Webber's time-saving capabilities for Data Ops.
"""
import os
import time
import requests
import webber
from webber.xcoms import Promise

def get_request(url: str):
    """Complete HTTP GET request and return text on success."""
    request = requests.get(url)
    if request.status_code == 200:
        print(f"{url} hit!")
        return request.text
    err_msg = f"Could not access endpoint {url}"
    raise RuntimeError(err_msg)

def write_content(url: str, text: str, filename: str):
    """Write text from a HTTP endpoint to file."""
    with open(filename, 'w+', encoding='utf-8') as _fd:
        _fd.write(text)
    print(f"{url} saved!")


if __name__ == "__main__":

    urls = ["https://google.com"] * 10

    print()
    print("Data R/W in Sequence:")
    sync_time = time.time()
    for u in urls:
        write_content(u, get_request(u), os.devnull)
    sync_time = time.time() - sync_time

    print()
    print("Data R/W in Webber:")
    dag = webber.DAG()
    for u in urls:
        getter = dag.add_node(get_request, u)
        writer = dag.add_node(write_content, u, Promise(getter), os.devnull)
        dag.add_edge(getter, writer)

    dag_time = time.time()
    dag.execute()
    dag_time = time.time() - dag_time

    print()
    print("Standard:", round(sync_time, 15), sep="\t ")
    print("Webber:\t", round(dag_time, 15), sep="\t ")
    print("Difference:\t",
        "+" if dag_time >= sync_time else "-", round(abs(dag_time - sync_time), 15),
        sep=""
    )

    dag.visualize()
