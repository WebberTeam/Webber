import webber
from dag_latency import first, second, third, fourth

dag = webber.DAG()
fun1 = dag.add_node(first)
fun2 = dag.add_node(second)
fun3 = dag.add_node(third)
fun4 = dag.add_node(fourth)

def show(*args):
    print(*args, sep='\n', end='\n\n')

show(1, *dag.nodes)

show(2, dag.nodes[fun1])

show(3, *dag.get_nodes())

show(4, dag.get_nodes(fun1))
show(5, *dag.get_nodes(fun1, fun2))
show(6, *dag.get_nodes([fun1, fun2]))
show(7, *dag.get_nodes([fun1, third]))
show(8, *dag.get_nodes(second, fourth))