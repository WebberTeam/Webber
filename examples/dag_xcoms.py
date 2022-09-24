"""
Demonstrates Webber's capacity for communication between dependent functions,
based on a temporary SQLite data store.
"""
from time import time
import webber
from webber.xcoms import Promise

def foo_1(context: Promise):                              # pylint: disable=missing-function-docstring
    context['my-arg'] = "Hi Mom!"
    print(f"From foo_1, my arg is: {context['my-arg']}")
    context.upload()

def foo_2(context: Promise):                              # pylint: disable=missing-function-docstring
    print(f"From foo_2, My arg was: {context['my-arg']}")

if __name__ == "__main__":
    dag = webber.DAG()
    f1 = dag.add_node(foo_1, context=Promise())
    f2 = dag.add_node(foo_2, context=Promise('my-arg'))
    dag.add_edge(f1, f2) # dag.add_edge(foo_1, foo_2) is equivalent in this scope

    webber_s = time()
    dag.execute()
    webber_time = time() - webber_s

    print("Webber Runtime:", webber_time)
