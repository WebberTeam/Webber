"""
Visualization library for Webber DAGs.
"""
import sys as _sys
import json as _json
import types as _types
import os.path as _path
import typing as _typing
import flask as _flask
import networkx as _nx
import webber.xcoms as _xcoms
from webber.edges import Condition
from jinja2 import Environment as _Environment, FileSystemLoader as _FileSystemLoader

# Wait for preload thread to finish to avoid circular imports with matplotlib/IPython
# Uses non-blocking check first; only blocks if preload is still running
import webber as _webber
if not _webber._viz_ready.is_set():
    _webber.wait_for_viz_ready(timeout=3.0)

import matplotlib.pyplot as _plt
from pyvis.network import Network as _Network
from netgraph import InteractiveGraph as _IGraph
from jinja2 import Environment as _Environment, FileSystemLoader as _FileSystemLoader

__all__ = ["generate_pyvis_network", "visualize_plt", "visualize_browser"]

edge_colors: _typing.Dict[Condition, str] = {
    Condition.Success: 'grey',
    Condition.AnyCase: 'blue',
    Condition.Failure: 'red'
}

def edge_color(c: Condition):
    """
    Given a Webber Condition, return corresponding color for edge visualizations.
    """
    return edge_colors[c]

def node_color(c: _typing.Callable):
    """
    Given a callable, return a color that to be used in visualizations
    mapping to the callable's type (lambda, function, built-in, class).
    """
    _class = str(c.__class__).strip("<class '").rstrip("'>")
    match _class:
        case 'type':
            return '#71C6B1'
        case 'function':
            return '#679AD1' if isinstance(c, _types.LambdaType) else '#DCDCAF'
        case 'builtin_function_or_method':
            return '#DCDCAF'
    return '#AADAFB'

def get_layers(graph: _nx.DiGraph) -> _typing.List[_typing.List[str]]:
    """
    Generates ordered list of node identifiers given a directed network graph.
    """
    layers = []
    for nodes in _nx.topological_generations(graph):
        layers.append(nodes)
    return layers

def annotate_node(node: _typing.Dict[str, _typing.Any]) -> str:
    """
    Given a Webber node, construct an annotation to be used in graph visualizations.
    """
    args, kwargs = [], {}
    for a in node['args']:
        try:
            args.append(_json.dumps(a))
        except:
            if isinstance(a, _xcoms.Promise):
                if isinstance(a.key, str):
                    name = a.key.split('__')[0]
                else:
                    name = a.key.__name__
                args.append(f'Promise({name})')
            else:
                args.append(f'Object({str(a.__class__)})')
    for k,v in node['kwargs'].items():
        try:
            _json.dumps(k)
            try:
                kwargs[_json.dumps(k)] = _json.dumps(v)
            except:
                if isinstance(v, _xcoms.Promise):
                    if isinstance(v.key, str):
                        name = v.key.split('__')[0]
                    else:
                        name = v.key.__name__
                    kwargs[k] = f"Promise('{name}')"
                else:
                    kwargs[k] = f'Object({str(v.__class__)})'
        except:
            pass
    node_title  = f"{node['name']}:"

    try:
        node_title += f" {node['callable'].__doc__.split('\n')[0]}"
    except:
        pass

    node_title += f"\nuuid:    {node['id'].split('__')[-1]}"
    node_title += f"\nposargs: {', '.join(args)}" if args else ""
    node_title += f"\nkwargs:  {_json.dumps(kwargs)}" if kwargs else ""

    return node_title

def visualize_plt(
    graph: _nx.DiGraph,
    interactive: bool = True,
    optimize_layout: bool = True
) -> _IGraph:
    """
    Generates basic network for visualization using the NetGraph library.

    Args:
        graph: NetworkX DiGraph to visualize
        interactive: If True, enables interactive mode in notebooks.
                     Ignored when using non-interactive backend (Agg).
        optimize_layout: If True, reduces edge crossings (slower but prettier).
                         Set to False for faster rendering on large graphs.
    """
    # Check if we're using a non-interactive backend
    import matplotlib
    backend = matplotlib.get_backend().lower()
    is_interactive_backend = backend not in ('agg', 'pdf', 'svg', 'ps', 'cairo')

    if _in_notebook() and interactive and is_interactive_backend:
        _plt.ion()
        _plt.close()
    return _IGraph(
        graph, arrows=True, node_shape='o', node_size=5,
        node_layout='multipartite',
        node_layout_kwargs=dict(layers=get_layers(graph), reduce_edge_crossings=optimize_layout),
        node_labels={id: c.__name__ for id,c in graph.nodes.data(data='callable')},
        node_color={id: node_color(c) for id,c in graph.nodes.data(data='callable')},
        edge_color={e[:-1]: edge_color(e[-1]) for e in graph.edges.data(data='Condition')},
        annotations={id: annotate_node(n) for id,n in graph.nodes.data(data=True)},
        annotation_fontdict=dict(horizontalalignment='left')
    )

def generate_pyvis_network(graph: _nx.DiGraph) -> _Network:
    """
    Generates basic network for visualization in Vis.js library.
    Depends on PyVis, Vis.js modules/libraries -- both are under legacy/minimal community support.
    """
    if len(graph.nodes()) == 0:
        err_msg = "Visualizations cannot be generated for DAGs without nodes."
        raise RuntimeError(err_msg)

    network = _Network(
        directed=True,
        layout='hierarchical'
    )
    network.inherit_edge_colors(False)

    generations = [sorted(generation) for generation in _nx.topological_generations(graph)]
    node_generation = lambda n: [i for i, G in enumerate(generations) if n in G][0]

    for n in graph.nodes:
        node = graph.nodes[n]
        network.add_node(
            n,
            label=node['name'],
            shape='box',
            title= annotate_node(node),
            labelHighlightBold=True,
            color=node_color(node['callable']),
            level=node_generation(n)
        )

    for source_edge, dest_edge in graph.edges:
        condition: Condition = graph.edges.get((source_edge, dest_edge))['Condition']
        network.add_edge(source_edge, dest_edge, color=edge_color(condition))

    return network


def generate_vis_js_script(graph: _nx.DiGraph) -> str:
    """
    Generates script for modeling Vis.js network graphs from a NetworkX DiGraph.
    Conformant to: Vis.js 4.20.1-SNAPSHOT
    """
    network: _Network = generate_pyvis_network(graph)
    network_data = dict(
        zip(["nodes", "edges", "heading", "height", "width", "options"],
        network.get_network_data())
    )

    script_js = "var nodes = new vis.DataSet(" + _json.dumps(network_data['nodes']) + """);\n"""
    script_js += "var edges = new vis.DataSet(" + _json.dumps(network_data['edges']) + """);\n"""
    script_js += """var container = document.getElementById("mynetwork");\n"""
    script_js += """var data = { nodes: nodes, edges: edges, };\n"""
    script_js += """var options = {
                    "autoResize": true,
                    "configure": {
                        "enabled": false
                    },
                    "edges": {
                        "color": {
                            "inherit": false
                        },
                        "smooth": {
                            "enabled": false,
                        },
                        "arrows": {
                            "to": true,
                            "from": true
                        }
                    },
                    "interaction": {
                        "dragNodes": true,
                        "hideEdgesOnDrag": false,
                        "hideNodesOnDrag": false
                    },
                    "layout": {
                        "hierarchical": {
                            "direction": "UD",
                            "blockShifting": true,
                            "edgeMinimization": false,
                            "enabled": true,
                            "parentCentralization": true,
                            "sortMethod": "hubsize",
                        },
                        "improvedLayout": true,
                        "randomSeed": 0,
                    },
                    "physics": {
                        "enabled": true,
                        "stabilization": {
                            "enabled": true,
                            "fit": true,
                            "iterations": 1000,
                            "onlyDynamicEdges": false,
                            "updateInterval": 50
                        }
                    }
                };\n"""
    script_js += """var network = new vis.Network(container, data, options);\n"""

    return script_js


def generate_vis_html(graph: _nx.DiGraph) -> str:
    """
    Generates HTML wrapper for Vis.js visualization -- used on both browser and GUI.
    """
    script = generate_vis_js_script(graph)
    if len(script) == 0:
        err_msg = "Empty JavaScript string given for Vis.js visualization."
        raise RuntimeError(err_msg)

    script = """<script type="text/javascript">\n""" + script + """</script>\n"""

    root = _path.dirname(_path.abspath(__file__))
    templates_dir = _path.join(root, 'templates')
    env = _Environment( loader = _FileSystemLoader(templates_dir) )
    template = env.get_template("vis_gui.html")

    return template.render(
        network_script = script,
    )


def visualize_browser(graph: _nx.DiGraph):
    """
    Visualizes Network graphs using a Flask app served to a desktop browser.
    """
    if _sys.platform not in ['darwin', 'win32', 'linux', 'linux2']:
        err_msg = "Unknown/unsupported operating system for GUI visualizations."
        raise RuntimeError(err_msg)

    gui_html = generate_vis_html(graph)

    server = _flask.Flask(__name__)
    server.add_url_rule("/", "index", lambda: gui_html)

    print('Serving visualization...\n')

    server.run(host="127.0.0.1", port=5000)

    print('\nVisualization closed.')

def visualize_dashboard(graph: _nx.DiGraph, metrics: _typing.Any) -> None:
    """
    Serves a post-execution dashboard showing the DAG with metrics overlay.
    Nodes are colored by execution status (green=completed, red=failed, grey=skipped).
    Sidebar shows timing data and summary statistics.

    Args:
        graph: The executed DAG's NetworkX DiGraph.
        metrics: A DAGMetrics instance from DAGExecutor.metrics.
    """
    if _sys.platform not in ['darwin', 'win32', 'linux', 'linux2']:
        err_msg = "Unknown/unsupported operating system for GUI visualizations."
        raise RuntimeError(err_msg)

    if len(graph.nodes()) == 0:
        raise RuntimeError("Cannot generate dashboard for empty DAG.")

    # Build Vis.js network with status-based coloring
    network = _Network(directed=True, layout='hierarchical')
    network.inherit_edge_colors(False)

    generations = [sorted(generation) for generation in _nx.topological_generations(graph)]
    node_generation = lambda n: [i for i, G in enumerate(generations) if n in G][0]

    status_colors = {
        'completed': '#4caf50',
        'failed': '#f44336',
        'skipped': '#9e9e9e',
    }

    for n in graph.nodes:
        node = graph.nodes[n]
        # Determine status color
        if n in metrics.completed:
            color = status_colors['completed']
        elif n in metrics.failed:
            color = status_colors['failed']
        elif n in metrics.skipped:
            color = status_colors['skipped']
        else:
            color = node_color(node['callable'])

        # Build tooltip with timing
        title = annotate_node(node)
        duration = metrics.task_durations.get(n)
        if duration is not None:
            title += f"\nduration: {duration*1000:.1f}ms"

        network.add_node(
            n, label=node['name'], shape='box',
            title=title, labelHighlightBold=True,
            color=color, level=node_generation(n)
        )

    for source_edge, dest_edge in graph.edges:
        condition: Condition = graph.edges.get((source_edge, dest_edge))['Condition']
        network.add_edge(source_edge, dest_edge, color=edge_color(condition))

    # Generate Vis.js script
    network_data = dict(
        zip(["nodes", "edges", "heading", "height", "width", "options"],
        network.get_network_data())
    )

    script_js = "var nodes = new vis.DataSet(" + _json.dumps(network_data['nodes']) + ");\n"
    script_js += "var edges = new vis.DataSet(" + _json.dumps(network_data['edges']) + ");\n"
    script_js += 'var container = document.getElementById("mynetwork");\n'
    script_js += "var data = { nodes: nodes, edges: edges };\n"
    script_js += """var options = {
        "autoResize": true,
        "edges": { "color": { "inherit": false }, "smooth": { "enabled": false }, "arrows": { "to": true } },
        "layout": { "hierarchical": { "direction": "UD", "enabled": true, "sortMethod": "hubsize" } },
        "physics": { "enabled": true, "stabilization": { "enabled": true, "iterations": 500 } }
    };\n"""
    script_js += "var network = new vis.Network(container, data, options);\n"

    script = '<script type="text/javascript">\n' + script_js + '</script>\n'

    # Build task list sorted by duration
    task_list = []
    for node_id, duration in sorted(metrics.task_durations.items(), key=lambda x: x[1], reverse=True):
        name = graph.nodes[node_id].get('name', node_id.split('__')[0])
        task_list.append({'id': node_id, 'name': name, 'duration_ms': duration * 1000})

    # Render template
    root = _path.dirname(_path.abspath(__file__))
    templates_dir = _path.join(root, 'templates')
    env = _Environment(loader=_FileSystemLoader(templates_dir))
    template = env.get_template("dashboard.html")

    html = template.render(
        network_script=script,
        total_duration=metrics.total_duration,
        completed_count=metrics.completed_count,
        failed_count=metrics.failed_count,
        skipped_count=metrics.skipped_count,
        total_nodes=metrics.total_nodes,
        task_list=task_list,
    )

    # Serve via Flask
    server = _flask.Flask(__name__)
    server.add_url_rule("/", "index", lambda: html)
    print('Serving execution dashboard...\n')
    server.run(host="127.0.0.1", port=5000)
    print('\nDashboard closed.')


def render_ascii(
    graph: _nx.DiGraph,
    metrics: _typing.Optional[_typing.Any] = None,
) -> str:
    """
    Render a DAG as ASCII/Unicode box art in the terminal.
    If metrics (DAGMetrics) is provided, nodes are colored by execution status:
    green=completed, red=failed, grey=skipped, with per-task timing labels.

    Requires phart >= 1.3.0 with BBOX support.
    Install: pip install "phart @ git+https://github.com/scottvr/phart.git@release-1.3.0"

    Args:
        graph: The DAG's NetworkX DiGraph (dag.graph).
        metrics: Optional DAGMetrics from executor.metrics.

    Returns:
        Rendered string (with ANSI codes if metrics provided).

    Raises:
        ImportError: If phart is not installed or lacks BBOX support.
    """
    try:
        from phart import ASCIIRenderer, LayoutOptions, NodeStyle
        if not hasattr(NodeStyle, 'BBOX'):
            raise ImportError("phart >= 1.3.0 with BBOX support required")
    except ImportError:
        raise ImportError(
            "ASCII DAG rendering requires phart >= 1.3.0.\n"
            "Install: pip install \"phart @ git+https://github.com/scottvr/phart.git@release-1.3.0\""
        )

    # Build display graph with human-readable names
    display = _nx.DiGraph()
    name_map: _typing.Dict[str, str] = {}
    for n in graph.nodes:
        name = graph.nodes[n].get('name', str(n).split('__')[0])
        if metrics:
            dur = metrics.task_durations.get(n)
            if dur is not None:
                label = f'{name} ({dur*1000:.0f}ms)'
            elif n in metrics.skipped:
                label = f'{name} [skip]'
            else:
                label = name
        else:
            label = name
        name_map[n] = label
        display.add_node(label)
    for u, v in graph.edges:
        display.add_edge(name_map[u], name_map[v])

    use_color = metrics is not None

    opts = LayoutOptions(
        bboxes=True, hpad=2, node_style=NodeStyle.MINIMAL,
        layer_spacing=3, edge_anchor_mode='ports', use_ascii=False,
        ansi_colors=use_color, color_nodes=True,
    )

    if not use_color:
        return ASCIIRenderer(display, options=opts).render()

    # Subclass to inject status colors
    _GREEN = '\033[38;5;34m'
    _RED = '\033[38;5;196m'
    _GREY = '\033[38;5;245m'
    _YELLOW = '\033[38;5;220m'

    class _StatusRenderer(ASCIIRenderer):
        def __init__(self, g, status_colors, **kw):
            self._status_colors = status_colors
            super().__init__(g, **kw)
        def _initialize_color_maps(self, positions):
            super()._initialize_color_maps(positions)
            self._node_color_map = dict(self._status_colors)
            for edge in self.graph.edges:
                self._edge_color_map[edge] = self._status_colors.get(edge[0], '')

    colors = {}
    for node_id, label in name_map.items():
        if node_id in metrics.completed:
            colors[label] = _GREEN
        elif node_id in metrics.failed:
            colors[label] = _RED
        elif node_id in metrics.skipped:
            colors[label] = _GREY
        else:
            colors[label] = _YELLOW

    return _StatusRenderer(display, status_colors=colors, options=opts).render()


def _in_notebook() -> bool:
    """
    Internal only. Helper to default to interactive notebooks when available
    if visualization type is not specified.
    """
    try:
        from IPython.core.getipython import get_ipython
        if 'IPKernelApp' not in get_ipython().config:
            return False
    except ImportError:
        return False
    except AttributeError:
        return False
    return True
