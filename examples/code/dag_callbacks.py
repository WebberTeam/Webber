"""
Demonstrates DAGCallbacks: observe DAG execution progress in real time
without modifying your callables.

All callback fields are optional — set only what you need.
"""
import time
import webber
from webber import Promise, DAGCallbacks

def extract():
    time.sleep(0.1)
    return [1, 2, 3, 4, 5]

def transform(data):
    time.sleep(0.05)
    return [x * 10 for x in data]

def load(data):
    time.sleep(0.05)
    return sum(data)

if __name__ == "__main__":
    print()

    callbacks = DAGCallbacks(
        on_dag_start=lambda total: print(f"DAG started: {total} nodes"),
        on_task_start=lambda nid, name: print(f"  -> Starting: {name}"),
        on_task_complete=lambda nid, name, result: print(f"  <- Complete: {name} => {result}"),
        on_task_error=lambda nid, name, exc: print(f"  !! Error: {name} => {exc}"),
        on_progress=lambda c, f, s, t: print(f"     Progress: {c+f+s}/{t}"),
        on_dag_complete=lambda c, f, s, d: print(f"\nDAG finished in {d*1000:.1f}ms ({c} ok, {f} failed, {s} skipped)"),
    )

    dag = webber.DAG()

    e = dag.add_node(extract)
    t = dag.add_node(transform, Promise(e))
    l = dag.add_node(load, Promise(t))

    dag.add_edge(e, t)
    dag.add_edge(t, l)

    dag.execute(callbacks=callbacks)
