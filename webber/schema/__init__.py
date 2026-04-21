"""
Webber topology serialization — export and import DAG structure as JSON-safe dicts.

Callables are NOT serialized (lambdas/closures cannot be pickled). Instead, topology
exports save the graph shape, edge conditions, and node metadata. Reconstruction
requires a callable registry mapping node names to functions.
"""
import json as _json
import typing as _T
import types as _types

from webber.edges import Condition
from webber.xcoms import Promise

__all__ = ["export_topology", "import_topology"]


def _serialize_arg(arg: _T.Any) -> _T.Any:
    """Serialize a single argument value. Promises become sentinel dicts."""
    if isinstance(arg, Promise):
        return {"__promise__": arg.key}
    # Try JSON-safe check
    try:
        _json.dumps(arg)
        return arg
    except (TypeError, ValueError):
        raise TypeError(
            f"Argument value {arg!r} (type {type(arg).__name__}) is not JSON-serializable. "
            f"Only JSON-safe args/kwargs can be exported in topology."
        )


def _deserialize_arg(arg: _T.Any) -> _T.Any:
    """Deserialize a single argument value. Sentinel dicts become Promises."""
    if isinstance(arg, dict) and "__promise__" in arg:
        return Promise(arg["__promise__"])
    return arg


def export_topology(dag: _T.Any) -> dict:
    """
    Export a DAG's topology as a JSON-serializable dictionary.

    Preserves: node IDs, names, args (JSON-safe), kwargs (JSON-safe),
    skip/retry config, edge conditions, and Promise references.

    Does NOT preserve: callable objects.

    Args:
        dag: A webber.DAG instance.

    Returns:
        A dictionary suitable for json.dumps().

    Raises:
        TypeError: If any args/kwargs are not JSON-serializable.
    """
    nodes = []
    for node_id, data in dag.graph.nodes(data=True):
        node_entry: dict = {
            "id": node_id,
            "name": data.get("name", ""),
        }

        # Serialize args
        args = data.get("args", ())
        node_entry["args"] = [_serialize_arg(a) for a in args]

        # Serialize kwargs
        kwargs = data.get("kwargs", {})
        node_entry["kwargs"] = {k: _serialize_arg(v) for k, v in kwargs.items()}

        # Optional metadata
        skip = data.get("skip")
        if skip and skip != (False, False):
            node_entry["skip"] = list(skip)

        retry = data.get("retry")
        if retry and retry > 0:
            node_entry["retry"] = retry

        nodes.append(node_entry)

    edges = []
    for u, v, data in dag.graph.edges(data=True):
        edge_entry: dict = {
            "parent": u,
            "child": v,
            "condition": data.get("Condition", Condition.Success).value,
        }
        edges.append(edge_entry)

    return {
        "version": "0.4",
        "nodes": nodes,
        "edges": edges,
    }


def import_topology(
    data: dict,
    registry: _T.Dict[str, _T.Callable] | None = None,
    module: _types.ModuleType | None = None,
) -> _T.Any:
    """
    Reconstruct a DAG from an exported topology dictionary.

    Requires a callable registry (explicit dict) or a module for auto-resolution.
    Explicit registry takes priority; module is a fallback for unresolved names.

    Args:
        data: Topology dict (from export_topology or JSON).
        registry: Dict mapping node names to callables. Primary resolution method.
        module: Python module to resolve callables by name (getattr fallback).

    Returns:
        A new webber.DAG instance with all nodes and edges restored.

    Raises:
        ValueError: If any node's callable cannot be resolved.
    """
    # Import here to avoid circular dependency
    from webber.core import DAG

    if registry is None and module is None:
        raise ValueError("Either registry dict or module must be provided to resolve callables.")

    effective_registry: _T.Dict[str, _T.Callable] = dict(registry) if registry else {}

    # Resolve all callables first to fail fast
    nodes_data = data.get("nodes", [])
    missing: list[str] = []

    for node_entry in nodes_data:
        name = node_entry["name"]
        if name not in effective_registry:
            if module is not None and hasattr(module, name):
                effective_registry[name] = getattr(module, name)
            else:
                missing.append(name)

    if missing:
        raise ValueError(
            f"Cannot resolve callables for nodes: {missing}. "
            f"Provide them in the registry dict or ensure they exist in the module."
        )

    # Build the DAG in two passes:
    # Pass 1: Add all nodes WITHOUT Promise args (to avoid resolve_promise failing on old IDs)
    # Pass 2: Patch args/kwargs with remapped Promises
    dag = DAG()
    id_map: _T.Dict[str, str] = {}  # old_id -> new_id
    deferred_args: _T.Dict[str, _T.Tuple[list, dict]] = {}  # new_id -> (args_with_promises, kwargs_with_promises)

    for node_entry in nodes_data:
        name = node_entry["name"]
        callable_fn = effective_registry[name]

        # Separate Promise args from plain args
        raw_args = node_entry.get("args", [])
        raw_kwargs = node_entry.get("kwargs", {})
        has_promises = any(
            (isinstance(a, dict) and "__promise__" in a) for a in raw_args
        ) or any(
            (isinstance(v, dict) and "__promise__" in v) for v in raw_kwargs.values()
        )

        if has_promises:
            # Add node with no args, will patch later
            new_id = dag.add_node(callable_fn)
            deferred_args[new_id] = (raw_args, raw_kwargs)
        else:
            # Safe to add directly — no Promises
            plain_args = [_deserialize_arg(a) for a in raw_args]
            plain_kwargs = {k: _deserialize_arg(v) for k, v in raw_kwargs.items()}
            new_id = dag.add_node(callable_fn, *plain_args, **plain_kwargs)

        id_map[node_entry["id"]] = new_id

        # Apply optional metadata
        if node_entry.get("skip"):
            skip_val = node_entry["skip"]
            dag.skip_node(new_id, skip=skip_val[0], as_failure=skip_val[1])
        if node_entry.get("retry"):
            dag.retry_node(new_id, count=node_entry["retry"])

        # Relabel to match original name
        dag.relabel_node(new_id, name)

    # Pass 2: Patch deferred Promise args using the id_map
    for new_id, (raw_args, raw_kwargs) in deferred_args.items():
        patched_args = []
        for a in raw_args:
            if isinstance(a, dict) and "__promise__" in a:
                old_ref = a["__promise__"]
                new_ref = id_map.get(old_ref, old_ref)
                patched_args.append(Promise(new_ref))
            else:
                patched_args.append(_deserialize_arg(a))

        patched_kwargs = {}
        for k, v in raw_kwargs.items():
            if isinstance(v, dict) and "__promise__" in v:
                old_ref = v["__promise__"]
                new_ref = id_map.get(old_ref, old_ref)
                patched_kwargs[k] = Promise(new_ref)
            else:
                patched_kwargs[k] = _deserialize_arg(v)

        dag.graph.nodes[new_id]['args'] = tuple(patched_args)
        dag.graph.nodes[new_id]['kwargs'] = patched_kwargs

    # Add edges using mapped IDs
    for edge_entry in data.get("edges", []):
        old_parent = edge_entry["parent"]
        old_child = edge_entry["child"]
        condition = Condition(edge_entry["condition"])

        new_parent = id_map.get(old_parent)
        new_child = id_map.get(old_child)

        if new_parent is None or new_child is None:
            raise ValueError(
                f"Edge references unknown node IDs: parent={old_parent}, child={old_child}"
            )

        dag.add_edge(new_parent, new_child, continue_on=condition)

    return dag
