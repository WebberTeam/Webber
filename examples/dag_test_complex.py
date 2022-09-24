"""
A simple example of Webber's functionality --
Compares overhead of synchronous exection in Webber with Python proper,
generally on the magnitude of hundreths of seconds.
"""
from time import sleep, time
from typing import List, Union
from webber import DAG
from webber.xcoms import Promise

def adder(nums):       # pylint: disable=missing-function-docstring
    total = sum(nums)
    print(total)
    return total

def muxer(nums):       # pylint: disable=missing-function-docstring
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
    def adder_wrapper(vals_to_sum, context: Promise, store_var: str):                # pylint: disable=missing-function-docstring
        context[store_var] = adder(vals_to_sum)
        context.upload()

    def muxer_wrapper(context_to_mux: List[str], context: Promise, store_var: str):  # pylint: disable=missing-function-docstring
        vals = [context[val] for val in context_to_mux]
        context[store_var] = muxer(vals)
        context.upload()

    dag = DAG()
    adder1 = dag.add_node(
        adder_wrapper, [1, 2, 3, 4],
        context=Promise(), store_var='sum1'
    )
    adder2 = dag.add_node(
        adder_wrapper, [5, 6, 7, 8],
        context=Promise(), store_var='sum2'
    )
    muxer1 = dag.add_node(
        muxer_wrapper, ['sum1', 'sum2'],
        context=Promise('sum1', 'sum2'), store_var='mux1_2'
    )
    dag.add_edge(adder1, muxer1)
    dag.add_edge(adder2, muxer1)

    start_time = time()
    add1 = adder([1,2,3,4])
    add2 = adder([5,6,7,8])
    muxer1 = muxer([add1, add2])
    run_time = time() - start_time
    print("Std runtime:", run_time)

    print()

    start_time = time()
    dag.execute()
    run_time = time() - start_time
    print("DAG runtime:", run_time)
