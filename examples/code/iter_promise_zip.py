"""
Demonstrates IterPromise zip mode: fan-out over multiple parent lists zipped element-wise.

Two source nodes produce lists of names and scores. The worker receives
paired tuples (name, score) and formats them. The collector aggregates.
"""
import webber
from webber import Promise, IterPromise

def get_students():
    return ["Alice", "Bob", "Charlie"]

def get_scores():
    return [95, 82, 91]

def format_report(pair):
    name, score = pair
    grade = "A" if score >= 90 else "B" if score >= 80 else "C"
    report = f"{name}: {score} ({grade})"
    print(f"  {report}")
    return report

def print_reports(reports):
    print(f"\nFinal report card ({len(reports)} students):")
    for r in reports:
        print(f"  {r}")

if __name__ == "__main__":
    print()

    dag = webber.DAG()

    names = dag.add_node(get_students)
    scores = dag.add_node(get_scores)
    worker = dag.add_node(format_report, IterPromise(names, scores, zip=True))
    report = dag.add_node(print_reports, Promise(worker))

    dag.add_edge(names, worker)
    dag.add_edge(scores, worker)
    dag.add_edge(worker, report)

    dag.execute()
