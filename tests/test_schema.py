"""Tests for DAG topology export/import (webber.schema)."""
import json
import os
import tempfile
import pytest
import webber
from webber import DAG, Promise
from webber.edges import Condition
from webber.schema import export_topology, import_topology


# --- Module-level named functions for registry testing ---

def extract():
    return [1, 2, 3]

def transform(data):
    return [x * 2 for x in data]

def load(data):
    return sum(data)

def failing_task():
    raise ValueError("intentional")


class TestExportTopology:
    """Tests for exporting DAG structure."""

    def test_export_simple_dag(self):
        """Export a DAG with two nodes and one edge."""
        dag = DAG()
        n1 = dag.add_node(extract)
        n2 = dag.add_node(transform, Promise(n1))
        dag.add_edge(n1, n2)

        topo = dag.to_dict()

        assert topo["version"] == "0.4"
        assert len(topo["nodes"]) == 2
        assert len(topo["edges"]) == 1

    def test_export_preserves_node_names(self):
        """Exported nodes should have their function names."""
        dag = DAG()
        dag.add_node(extract)
        dag.add_node(transform)

        topo = dag.to_dict()
        names = [n["name"] for n in topo["nodes"]]

        assert "extract" in names
        assert "transform" in names

    def test_export_preserves_edge_conditions(self):
        """Edge conditions should be serialized as integers."""
        dag = DAG()
        n1 = dag.add_node(extract)
        n2 = dag.add_node(transform)
        dag.add_edge(n1, n2, continue_on=Condition.Failure)

        topo = dag.to_dict()

        assert topo["edges"][0]["condition"] == Condition.Failure.value

    def test_export_preserves_json_safe_args(self):
        """JSON-serializable args should be preserved."""
        dag = DAG()
        dag.add_node(transform, [1, 2, 3])

        topo = dag.to_dict()
        node = topo["nodes"][0]

        assert node["args"] == [[1, 2, 3]]

    def test_export_preserves_kwargs(self):
        """JSON-serializable kwargs should be preserved."""
        def my_func(x=10, y="hello"):
            return x

        dag = DAG()
        dag.add_node(my_func, x=42, y="world")

        topo = dag.to_dict()
        node = topo["nodes"][0]

        assert node["kwargs"] == {"x": 42, "y": "world"}

    def test_export_promises_as_sentinels(self):
        """Promise objects in args should serialize as __promise__ sentinels."""
        dag = DAG()
        n1 = dag.add_node(extract)
        n2 = dag.add_node(transform, Promise(n1))
        dag.add_edge(n1, n2)

        topo = dag.to_dict()
        transform_node = next(n for n in topo["nodes"] if n["name"] == "transform")

        assert len(transform_node["args"]) == 1
        assert "__promise__" in transform_node["args"][0]

    def test_export_preserves_retry(self):
        """Retry config should be included in export."""
        dag = DAG()
        n1 = dag.add_node(extract)
        dag.retry_node(n1, count=3)

        topo = dag.to_dict()
        assert topo["nodes"][0]["retry"] == 3

    def test_export_preserves_skip(self):
        """Skip config should be included in export."""
        dag = DAG()
        n1 = dag.add_node(extract)
        dag.skip_node(n1, skip=True, as_failure=True)

        topo = dag.to_dict()
        assert topo["nodes"][0]["skip"] == [True, True]

    def test_export_non_serializable_args_raises(self):
        """Non-JSON-serializable args should raise TypeError."""
        dag = DAG()
        dag.add_node(transform, object())

        with pytest.raises(TypeError, match="not JSON-serializable"):
            dag.to_dict()

    def test_export_is_json_serializable(self):
        """Entire topology dict should be valid JSON."""
        dag = DAG()
        n1 = dag.add_node(extract)
        n2 = dag.add_node(transform, Promise(n1))
        dag.add_edge(n1, n2)

        topo = dag.to_dict()
        json_str = json.dumps(topo)  # Should not raise
        assert isinstance(json_str, str)


class TestImportTopology:
    """Tests for importing/reconstructing DAGs from topology."""

    def test_import_with_registry(self):
        """Import should reconstruct DAG using explicit registry."""
        dag = DAG()
        n1 = dag.add_node(extract)
        n2 = dag.add_node(transform)
        dag.add_edge(n1, n2)

        topo = dag.to_dict()
        registry = {"extract": extract, "transform": transform}

        dag2 = DAG.from_dict(topo, registry=registry)
        assert len(dag2.graph.nodes) == 2
        assert len(dag2.graph.edges) == 1

    def test_import_with_module(self):
        """Import should resolve callables from a module."""
        import tests.test_schema as this_module

        dag = DAG()
        n1 = dag.add_node(extract)
        n2 = dag.add_node(transform)
        dag.add_edge(n1, n2)

        topo = dag.to_dict()
        dag2 = DAG.from_dict(topo, module=this_module)

        assert len(dag2.graph.nodes) == 2
        assert len(dag2.graph.edges) == 1

    def test_import_missing_callable_raises(self):
        """Import should raise ValueError listing missing callables."""
        topo = {
            "version": "0.4",
            "nodes": [{"id": "x", "name": "nonexistent", "args": [], "kwargs": {}}],
            "edges": [],
        }

        with pytest.raises(ValueError, match="nonexistent"):
            DAG.from_dict(topo, registry={})

    def test_import_no_registry_or_module_raises(self):
        """Import without registry or module should raise ValueError."""
        topo = {"version": "0.4", "nodes": [], "edges": []}

        with pytest.raises(ValueError, match="Either registry dict or module"):
            DAG.from_dict(topo, registry=None, module=None)

    def test_import_preserves_edge_conditions(self):
        """Imported edges should have correct conditions."""
        dag = DAG()
        n1 = dag.add_node(extract)
        n2 = dag.add_node(transform)
        dag.add_edge(n1, n2, continue_on=Condition.Failure)

        topo = dag.to_dict()
        registry = {"extract": extract, "transform": transform}
        dag2 = DAG.from_dict(topo, registry=registry)

        edges = dag2.get_edges()
        assert edges[0].Condition == Condition.Failure

    def test_import_preserves_retry(self):
        """Imported nodes should have retry config restored."""
        dag = DAG()
        n1 = dag.add_node(extract)
        dag.retry_node(n1, count=5)

        topo = dag.to_dict()
        registry = {"extract": extract}
        dag2 = DAG.from_dict(topo, registry=registry)

        node = list(dag2.graph.nodes(data=True))[0][1]
        assert node.get("retry") == 5

    def test_roundtrip_execute(self):
        """Exported then imported DAG should execute successfully."""
        dag = DAG()
        n1 = dag.add_node(extract)
        n2 = dag.add_node(load, Promise(n1))
        dag.add_edge(n1, n2)

        topo = dag.to_dict()
        registry = {"extract": extract, "load": load}
        dag2 = DAG.from_dict(topo, registry=registry)

        # Should not raise
        dag2.execute()


class TestJsonFileIO:
    """Tests for JSON file read/write."""

    def test_to_json_returns_string(self):
        """to_json should return a valid JSON string."""
        dag = DAG()
        dag.add_node(extract)

        result = dag.to_json()
        parsed = json.loads(result)
        assert parsed["version"] == "0.4"

    def test_to_json_writes_file(self):
        """to_json with file path should write to disk."""
        dag = DAG()
        dag.add_node(extract)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            path = f.name

        try:
            dag.to_json(file=path)
            with open(path, 'r') as f:
                parsed = json.load(f)
            assert parsed["version"] == "0.4"
        finally:
            os.unlink(path)

    def test_from_json_file(self):
        """from_json should read from a file path."""
        dag = DAG()
        dag.add_node(extract)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            path = f.name

        try:
            dag.to_json(file=path)
            dag2 = DAG.from_json(path, registry={"extract": extract})
            assert len(dag2.graph.nodes) == 1
        finally:
            os.unlink(path)

    def test_from_json_string(self):
        """from_json should accept a JSON string directly."""
        dag = DAG()
        dag.add_node(extract)

        json_str = dag.to_json()
        dag2 = DAG.from_json(json_str, registry={"extract": extract})

        assert len(dag2.graph.nodes) == 1

    def test_full_roundtrip_file(self):
        """Full roundtrip: build → export to file → import from file → execute."""
        dag = DAG()
        n1 = dag.add_node(extract)
        n2 = dag.add_node(transform, Promise(n1))
        n3 = dag.add_node(load, Promise(n2))
        dag.add_edge(n1, n2)
        dag.add_edge(n2, n3)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            path = f.name

        try:
            dag.to_json(file=path)
            registry = {"extract": extract, "transform": transform, "load": load}
            dag2 = DAG.from_json(path, registry=registry)

            assert len(dag2.graph.nodes) == 3
            assert len(dag2.graph.edges) == 2
            dag2.execute()  # Should not raise
        finally:
            os.unlink(path)
