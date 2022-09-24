# Webber
An Pythonic platform for no-latency ETL and parallelization inspired by Apache Airflow.


# When Should I Use Webber?

Although Webber is relatively low-cost in comparison to other parallelization frameworks in Python, it run *many magnitudes* slower than frameworks available in other high-level languages, such as Rust, C/C++, or Java.

At this time, there are two overarching use-cases.

1. I want to experiment with structured multiprocessing in Python.
2. Or...
<ul style="margin-left:30px;">
    <li>I have a Pythonic workflow that needs to be restructured in parallel.</li>
    <li>My workflow has inter-function dependencies that cannot be easily decoupled without database/file-stores.</li>
    <li>My workflow is large enough that I can allow for sub-second latency between events.</li>
</ul>