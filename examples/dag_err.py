"""
Simple demo of error handling in Webber DAGs:

- Tracebacks are printed.

- Dependent tasks are skipped.

- The DAG continues to execute independent tasks.
"""
import sys
import webber

def erroneous():
    """Force an exit."""
    print("I am an error.")
    sys.exit(1)

def independent():
    """Make a statement."""
    print("I am independent.")

def dependent():
    """Make a statement (if you can!)"""
    print("I am dependent on erroneous.")

def follower():
    """Make a statement after erroneous."""
    print("I should follow erroneous, but I'm not dependent on its success.")

if __name__ == "__main__":

    dag = webber.DAG()

    err_event: str = dag.add_node(erroneous)
    ind_event: str = dag.add_node(independent)
    dep_event: str = dag.add_node(dependent)

    _ = dag.add_edge(err_event, dependent)
    _ = dag.add_edge(err_event, follower, continue_on=webber.Condition.AnyCase)
    _ = dag.add_edge(independent, dependent)

    dag.execute(print_exc=True)
    dag.visualize()