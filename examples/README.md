# Where Should I Start?

## New to Python?

#### This module assumes that you are comfortable using Python as a programming language and have at least some exposure to dynamic programming.

If you're brand new to programming, we recommend that you go through at least one of these free courses before progressing any further:

- [Harvard CS50: Intro to Programming with Python](https://cs50.harvard.edu/python/2022/)
- [MIT 6.100L: Intro to CS and Programming using Python](https://ocw.mit.edu/courses/6-100l-introduction-to-cs-and-programming-using-python-fall-2022/)

For those with basic exposure to Python, please reference [Python 3's official documentation](https://docs.python.org/3/):
- [Functional Programming](https://docs.python.org/3/howto/functional.html)
- [Data Structures](https://docs.python.org/3/tutorial/datastructures.html)

## Starting with Webber

For all developers, we ***strongly*** recommend a few introductory readings on Directed Acyclic Graphs (DAGs) and dynamic programming:

- [James Davis explains DAGs in 180 seconds (YouTube)](https://www.youtube.com/watch?v=Y4hlrkidWQo)
- [Dynamic Programming from *Algorithms*, by Dasgupta, Papadimitriou, and Vazirani](https://people.eecs.berkeley.edu/~vazirani/algorithms/chap6.pdf)
  - [The full text is available for review here.](http://algorithmics.lsi.upc.edu/docs/Dasgupta-Papadimitriou-Vazirani.pdf)

Then, make sure you've installed the latest stable release of Webber and its dependencies:
```
python -m pip install webber
```

Once Webber is installed, we recommend going through the following Jupyter notebooks in an interactive development environment (IDE) of your choice:
1. [Introduction to Webber](./notebooks/webber-intro.ipynb)
2. [Using Promises in Webber](./notebooks/more-dag-tests.ipynb)
3. [Conditional Execution and Skips in Webber](./notebooks/skip-dags.ipynb)

From there, you can reference the other examples in this repository for your specific use-case and experimentation. 

Some of our favorites examples:
- [XComs in Webber](./code/dag_xcoms.py)
- [Complex Use of Promises in Webber](./code/dag_complex.py)
- [Node Getters in Webber](./code/dag_get_nodes.py)

2024.10.11: We're still building this section out. We are actively accepting feedback and help in writing documentation and testing edge cases.

## Advanced Readings:

Referencing [Apache Airflow's](https://airflow.apache.org/docs/apache-airflow/stable/index.html) documentation on directed acyclic graphs and writing "workflows as code" may be helpful for industry software engineers:
- [Airflow Architecture Overview](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html)
- [Airflow Core Concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)

For those with an academic interest in DAGs and dyanmic programming, we recommend a few advanced readings:

- [Lecture Notes from Dr. Qing Zhao @ UCLA](http://www.stat.ucla.edu/~zhou/index.html)
- [Zhao et. al - DAG Scheduling and Analysis on Multiprocessor Systems](https://eprints.whiterose.ac.uk/167629/1/rtss2020_dag.pdf)
- [Wikipedia - Topological sorting (Application to shortest path finding)](https://en.wikipedia.org/wiki/Topological_sorting#Application_to_shortest_path_finding)
- [Matúš Bezek - Characterizing DAG-depth of Directed Graphs](https://arxiv.org/pdf/1612.04980)
- [MIT OpenCourseWare - Directed Acyclic Graphs & Scheduling](https://ocw.mit.edu/courses/6-042j-mathematics-for-computer-science-spring-2015/mit6_042js15_session18.pdf)
  - [See also: Albert Meyers' associated lecture series on YouTube](https://www.youtube.com/watch?v=Sdw8_0RDZuw)
