import webber.etl as etl
from webber import Promise

def writer(x):
    print(x)
    fd = open('myfile.txt', 'a+')
    fd.writelines(x + "\n")
    fd.close()


dag = etl.AsyncDAG()
x = dag.add_node(lambda: "1", iterator=4)
y = dag.add_node(writer, Promise(x))
dag.add_edge(x, y)
dag.execute()