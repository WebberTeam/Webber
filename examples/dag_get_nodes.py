import webber
from dag_latency import first, second, third, fourth

dag = webber.DAG()
fun1 = dag.add_node(first)
fun2 = dag.add_node(second)
fun3 = dag.add_node(third)
fun4 = dag.add_node(fourth)

def show(title, *args):
    print(title)
    print('-----------------')
    print(*args, sep='\n')
    print('-----------------', end='\n\n')

show('dag.nodes', *dag.nodes)

show('dag.nodes[first]', dag.nodes[fun1])

show('dag.get_nodes()', *dag.get_nodes())

show('dag.get_nodes(first)', dag.get_nodes(fun1))
show('dag.get_nodes(first, second)', *dag.get_nodes(fun1, fun2))
show('dag.get_nodes([first, second])', *dag.get_nodes([fun1, fun2]))
show('dag.get_nodes(first, third_callable)', *dag.get_nodes([fun1, third]))
show('dag.get_nodes(second_callable, fourth_callable)', *dag.get_nodes(second, fourth))
