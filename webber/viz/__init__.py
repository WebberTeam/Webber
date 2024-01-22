"""
Visualization library for Webber DAGs.

Last updated by: Jan 22, 2024 (v0.0.2)
"""
import sys as _sys
import json as _json
import os.path as _path
import flask as _flask
import networkx as _nx
import webber.xcoms as _xcoms
import webber.edges as _edges
import matplotlib.pyplot as _plt
from pyvis.network import Network as _Network
# from PyQt6.QtWidgets import QApplication as _QApplication              # pylint: disable=no-name-in-module
# from PyQt6.QtWebEngineCore import QWebEnginePage as _QWebEnginePage    # pylint: disable=no-name-in-module
# from PyQt6.QtWebEngineWidgets import QWebEngineView as _QWebEngineView # pylint: disable=no-name-in-module

from jinja2 import Environment as _Environment, FileSystemLoader as _FileSystemLoader

__all__ = ["generate_pyvis_network", "visualize_plt", "visualize_browser"]

def visualize_plt(graph: _nx.DiGraph) -> list[str]:
    for layer, nodes in enumerate(_nx.topological_generations(graph)):
        for node in nodes:
            graph.nodes[node]["layer"] = layer
    graph = _nx.relabel_nodes(graph, lambda node: graph.nodes[node]["name"])
    pos = _nx.multipartite_layout(graph, subset_key="layer")
    fig, ax = _plt.subplots()
    _nx.draw_networkx(graph, pos=pos, ax=ax)
    ax.set_title("DAG layout in topological order")
    fig.tight_layout()
    _plt.show()

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
        layout='hierarchical',
    )

    for n in graph.nodes:
        node = graph.nodes[n]
        args, kwargs = [], {}
        for a in node['args']:
            try:
                args.append(_json.dumps(a))
            except:
                if isinstance(a, _xcoms.Promise):
                    args.append(f'Promise({a.key})')
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
            node_title += f" {node['callable'].__doc__}"
        except:
            pass

        node_title += f"<br>uuid:    {n.split('__')[-1]}"
        node_title += f"<br>posargs: {', '.join(args)}" if args else ""
        node_title += f"<br>kwargs:  {_json.dumps(kwargs)}" if kwargs else ""

        network.add_node(
            n,
            label=node['name'],
            shape='circle' if len(graph) < 4 else 'box',
            title= node_title,
            labelHighlightBold=True
        )
    for source_edge, dest_edge in graph.edges:
        network.add_edge(source_edge, dest_edge)
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
                            "inherit": true
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
                            "direction": "UD",                            "blockShifting": true,
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


# def visualize_gui(graph: _nx.DiGraph):
#     """
#     Visualizes Network graphs using a desktop GUI generated by the PyQt6 library.
#     """
#     if sys.platform not in ['darwin', 'win32', 'linux', 'linxu2']:
#         err_msg = "Unknown/unsupported operating system for GUI visualizations."
#         raise RuntimeError(err_msg)

#     gui_html = generate_vis_html(graph)

#     class WebEngineView(_QWebEngineView): # pylint: disable=too-few-public-methods
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
