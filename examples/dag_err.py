import webber

def erroneous():       # pylint: disable=missing-function-docstring
    exit(1)

def independent():
    print("I am independent.")

def dependent():
    print("I am dependent.")

if __name__ == "__main__":
    dag = webber.DAG()
    err_event: str = dag.add_node(erroneous)
    ind_event: str = dag.add_node(independent)
    dep_event: str = dag.add_node(dependent)
    _ = dag.add_edge(err_event, dependent)
    _ = dag.add_edge(independent, dependent)
    dag.execute()