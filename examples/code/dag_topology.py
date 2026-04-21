"""
Demonstrates Topology Export/Import: save DAG structure as JSON and
reconstruct it with a callable registry.

Useful for templating reusable pipeline shapes, sharing DAG definitions,
or storing workflow configurations.
"""
import json
import webber
from webber import Promise

# --- Pipeline functions (named, not lambdas — required for registry) ---

def extract():
    return [10, 20, 30, 40, 50]

def transform(data):
    return [x * 2 for x in data]

def load(data):
    print(f"Loaded {len(data)} items: {data}")
    return len(data)

if __name__ == "__main__":
    print()

    # Build original DAG
    dag = webber.DAG()
    e = dag.add_node(extract)
    t = dag.add_node(transform, Promise(e))
    l = dag.add_node(load, Promise(t))
    dag.add_edge(e, t)
    dag.add_edge(t, l)

    # Export as JSON
    json_str = dag.to_json()
    print("Exported topology:")
    print(json.dumps(json.loads(json_str), indent=2)[:500])
    print("...")

    # Import with explicit registry
    registry = {
        "extract": extract,
        "transform": transform,
        "load": load,
    }
    dag2 = webber.DAG.from_json(json_str, registry=registry)

    print(f"\nReconstructed DAG: {len(dag2.graph.nodes)} nodes, {len(dag2.graph.edges)} edges")
    print("Executing reconstructed DAG:")
    dag2.execute()

    # Can also import from a module directly:
    #   import my_pipeline
    #   dag3 = webber.DAG.from_json("pipeline.json", module=my_pipeline)
    # This resolves callables by matching node names to module attributes.
