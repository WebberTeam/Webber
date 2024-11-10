"""
Visualization library for Webber DAGs.

Last updated by: Jan 22, 2024 (v0.0.2)
"""
import sys as _sys
import json as _json
import types as _types
import os.path as _path
import typing as _typing
import flask as _flask
import networkx as _nx
import webber.xcoms as _xcoms
import matplotlib.pyplot as _plt
from webber.edges import Condition
from pyvis.network import Network as _Network
from netgraph import InteractiveGraph as _IGraph
# from PyQt6.QtWidgets import QApplication as _QApplication
# from PyQt6.QtWebEngineCore import QWebEnginePage as _QWebEnginePage
# from PyQt6.QtWebEngineWidgets import QWebEngineView as _QWebEngineView

from jinja2 import Environment as _Environment, FileSystemLoader as _FileSystemLoader

__all__ = ["generate_pyvis_network", "visualize_plt", "visualize_browser"]

edge_colors: dict[Condition, str] = {
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

def get_layers(graph: _nx.DiGraph) -> list[list[str]]:
    """
    Generates ordered list of node identifiers given a directed network graph.
    """
    layers = []
    for nodes in _nx.topological_generations(graph):
        layers.append(nodes)
    return layers

def annotate_node(node: dict):
    """
    Given a Webber node, construct an annotation to be used in graph visualizations.
    """
    args, kwargs = [], {}
    for a in node['args']:
        try:
            args.append(_json.dumps(a))
        except:
            if isinstance(a, _xcoms.Promise):
                args.append(f'Promise({a.key.split('__')[0]})')
            else:
                args.append(f'Object({str(a.__class__)})')
    for k,v in node['kwargs'].items():
        try:
            _json.dumps(k)
            try:
                kwargs[_json.dumps(k)] = _json.dumps(v)
            except:
                if isinstance(v, _xcoms.Promise):
                    kwargs[k] = f"Promise('{v.key.split('__')[0]}')"
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

def visualize_plt(graph: _nx.DiGraph, interactive=True) -> _IGraph:
    """
    Generates basic network for visualization using the NetGraph library.
    """
    if _in_notebook() and interactive:
        _plt.ion()
        _plt.close()
    return _IGraph(
        graph, arrows=True, node_shape='o', node_size=5,
        node_layout='multipartite',
        node_layout_kwargs=dict(layers=get_layers(graph), reduce_edge_crossings=True),
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

    # Invalid JavaScript check:
    # if not (script):
        # err_msg = "Invalid JavaScript string for Vis.js visualization."
        # raise RuntimeError(err_msg)

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
    if _sys.platform not in ['darwin', 'win32', 'linux', 'linxu2']:
        err_msg = "Unknown/unsupported operating system for GUI visualizations."
        raise RuntimeError(err_msg)

    gui_html = generate_vis_html(graph)

    server = _flask.Flask(__name__)
    server.add_url_rule("/", "index", lambda: gui_html)

    print('Serving visualization...\n')

    server.run(host="127.0.0.1", port=5000)

    print('\nVisualization closed.')

def _in_notebook() -> bool:
    """
    Internal only. Helper to default to interactive notebooks when available
    if visualization type is not specified.
    """
    try:
        from IPython import get_ipython
        if 'IPKernelApp' not in get_ipython().config:
            return False
    except ImportError:
        return False
    except AttributeError:
        return False
    return True

# def visualize_gui(graph: _nx.DiGraph):
#     """
#     Visualizes Network graphs using a desktop GUI generated by the PyQt6 library.
#     """
#     if sys.platform not in ['darwin', 'win32', 'linux', 'linxu2']:
#         err_msg = "Unknown/unsupported operating system for GUI visualizations."
#         raise RuntimeError(err_msg)

#     gui_html = generate_vis_html(graph)

#     class WebEngineView(_QWebEngineView):
#         """
#         A small Qt-based WebEngineView to generate a GUI using embedded HTML and JavaScript.
#         """
#         def __init__(self, parent=None):
#             super().__init__(parent)
#             self.webpage = _QWebEnginePage()
#             self.setPage(self.webpage)
#             self.webpage.setHtml(gui_html)

#     app = _QApplication([])
#     web_engine_view = WebEngineView()
#     web_engine_view.showNormal()
#     app.exec()
