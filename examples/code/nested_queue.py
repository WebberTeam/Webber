import webber.queue as queue
from webber import Promise, DAG
import time

def writer(x):
    fd = open('myfile.txt', 'a+')
    fd.writelines(str(x) + "\n")
    fd.close()


main_dag = DAG()
name = main_dag.add_node(lambda: "World")

queue_dag = queue.AsyncDAG()
greeting = queue_dag.add_node(lambda n: f"Hello, {n}", Promise(name), iterator=200)
writing  = queue_dag.add_node(writer, Promise(greeting))
queue_dag.add_edge(greeting, writing)


main_dag.add_node(queue_dag.execute, name, Promise(name), print_exc = True)
main_dag.add_edge(name, queue_dag.execute)

t = time.time()
main_dag.execute()
print(time.time() - t)

t1 = time.time()
queue_dag.execute(name, "World", print_exc=True)
print(time.time() - t1)

t2 = time.time()
for i in range(200):
    writer("Hello, World")
print(time.time() - t2)

