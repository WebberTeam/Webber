"""
A simple example of Webber's functionality --
Compares overhead of synchronous exection in Webber with Python proper,
generally on the magnitude of hundreths of seconds.
"""
from time import time, sleep
from networkx import DiGraph
from webber import DAG

def first():
    print("1")
    sleep(1)

def second():
    print("2")
    sleep(1)

def third():
    print("3")
    sleep(1)

def fourth():
    print("4")
    sleep(1)

if __name__ == "__main__":

    std_start = time()
    first()
    second()
    third()
    fourth()
    std_time = time() - std_start

    G = DiGraph([
        (first,second),
        (second,third),
        (third,fourth)
    ])

    dag = DAG(G)

    print()

    dag_start = time()
    dag.execute()
    dag_time = time() - dag_start

    print()

    print("Standard:", round(std_time, 15), sep="\t ")
    print("Webber:\t", round(dag_time, 15), sep="\t ")
    print("Difference:\t",
        "+" if dag_time >= std_time else "-", round(abs(dag_time - std_time), 15),
        sep=""
    )

    dag.visualize()
