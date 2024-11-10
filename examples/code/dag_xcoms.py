"""
Demonstrates Webber's capacity for communication between dependent functions.
"""
from time import time
import webber
from webber.xcoms import Promise

def introduction(name):
    print(f"My name is...{name}.")
    return name

def greeting(name):
    print(f"Hello, {name}!")

if __name__ == "__main__":

    print()

    dag = webber.DAG()
    intro = dag.add_node(introduction, "World")
    hello = dag.add_node(greeting, name=Promise(intro))
    dag.add_edge(intro, hello)

    webber_s = time()

    dag.execute()

    webber_time = time() - webber_s

    print("\nWebber Runtime:", webber_time, end="\n\n")

    dag.visualize()
