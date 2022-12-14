"""
Visualization library for Webber DAGs.

Last updated by: June 1, 2021 (pre-0.0.1)
"""
from json import dumps as _dumps
from sys import platform as _platform
from os.path import dirname as _dirname, abspath as _abspath, join as _join
from flask import Flask as _Flask
from networkx import DiGraph as _DiGraph
from pyvis.network import Network as _Network
from webber.xcoms import Promise as _Promise
# from PyQt6.QtWidgets import QApplication as _QApplication              # pylint: disable=no-name-in-module
# from PyQt6.QtWebEngineCore import QWebEnginePage as _QWebEnginePage    # pylint: disable=no-name-in-module
# from PyQt6.QtWebEngineWidgets import QWebEngineView as _QWebEngineView # pylint: disable=no-name-in-module

from jinja2 import Environment as _Environment, FileSystemLoader as _FileSystemLoader

def generate_pyvis_network(graph: _DiGraph) -> _Network:
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
                args.append(_dumps(a))
            except:
                if isinstance(a, _Promise):
                    args.append(f'Promise({a.key})')
                else:
                    args.append(f'Object({str(a.__class__)})')
        for k,v in node['kwargs'].items():
            try:
                _dumps(k)
                try:
                    kwargs[_dumps(k)] = _dumps(v)
                except:
                    if isinstance(v, _Promise):
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
        node_title += f"<br>kwargs:  {_dumps(kwargs)}" if kwargs else ""

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


def generate_vis_js_script(graph: _DiGraph) -> str:
    """
    Generates script for modeling Vis.js network graphs from a NetworkX DiGraph.

    Conformant to: Vis.js 4.20.1-SNAPSHOT
    """
    network: _Network = generate_pyvis_network(graph)
    network_data = dict(
        zip(["nodes", "edges", "heading", "height", "width", "options"],
        network.get_network_data())
    )

    script_js = "var nodes = new vis.DataSet(" + _dumps(network_data['nodes']) + """);\n"""
    script_js += "var edges = new vis.DataSet(" + _dumps(network_data['edges']) + """);\n"""
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


def generate_vis_html(graph: _DiGraph) -> str:
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

    root = _dirname(_abspath(__file__))
    templates_dir = _join(root, 'templates')
    env = _Environment( loader = _FileSystemLoader(templates_dir) )
    template = env.get_template("vis_gui.html")

    return template.render(
        network_script = script,
    )


def visualize_browser(graph: _DiGraph):
    """
    Visualizes Network graphs using a Flask app served to a desktop browser.
    """
    if _platform not in ['darwin', 'win32', 'linux', 'linxu2']:
        err_msg = "Unknown/unsupported operating system for GUI visualizations."
        raise RuntimeError(err_msg)

    gui_html = generate_vis_html(graph)

    server = _Flask(__name__)
    server.add_url_rule("/", "index", lambda: gui_html)

    print('Serving visualization...\n')

    server.run(host="127.0.0.1", port=5000)

    print('\nVisualization closed.')


# def visualize_gui(graph: _DiGraph):
#     """
#     Visualizes Network graphs using a desktop GUI generated by the PyQt6 library.
#     """
#     if _platform not in ['darwin', 'win32', 'linux', 'linxu2']:
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
