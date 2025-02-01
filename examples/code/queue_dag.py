import time
import webber

def writer(x):
    fd = open('myfile.txt', 'a+')
    fd.writelines(x + "\n")
    fd.close()


dag = webber.QueueDAG()
x = dag.add_node(lambda: "1", iterator=100)
y = dag.add_node(writer, webber.Promise(x))
dag.add_edge(x, y)
t = time.time()
dag.execute(print_exc=True)
print(time.time() - t)

t1 = time.time()
for i in range(100):
    writer("1")
print(time.time() - t1)