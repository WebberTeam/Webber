"""
A simple example of Webber's functionality --
Compares overhead of synchronous exection in Webber with Python proper,
generally on the magnitude of hundreths of seconds.
"""
from time import sleep, time
from typing import Union
from webber import DAG
from webber.xcoms import Promise

def adder(nums):       # pylint: disable=missing-function-docstring
    total = sum(nums)
    print(total)
    return total

def muxer(*nums):       # pylint: disable=missing-function-docstring
    mux = 1
    num = None
    for num in nums:
        mux *= num if num else 1
    print(mux)
    return mux

def print_and_wait(*values, sep=' ', end='\n', sleep_time: Union[int, float] = 0.0): # pylint: disable=missing-function-docstring
    print(*values, sep=sep, end=end)
    sleep(sleep_time)

if __name__ == "__main__":

    dag = DAG()

    add1 = dag.add_node(adder, [1, 2, 3, 4])
    add2 = dag.add_node(adder, [5, 6, 7, 8])
    mux3  = dag.add_node(muxer, Promise(add1), Promise(add2))

    dag.add_edge(add1, mux3)
    dag.add_edge(add2, mux3)

    dag_time = time()

    print()
    dag.execute()
    print()

    dag_time = time() - dag_time

    print("DAG runtime:", dag_time)
