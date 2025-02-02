from setuptools import setup, find_packages
setup(
    name='webber',
    version='0.2.0',
    description='An Pythonic platform for no-latency ETL and parallelization inspired by Apache Airflow.',
    long_description='An Pythonic platform for no-latency ETL and parallelization inspired by Apache Airflow.',
    long_description_content_type='text/x-rst',
    packages=find_packages(include=['webber', 'webber.*']),
    install_requires=["networkx", "flask", "pyvis", "jinja2", "matplotlib", "ipympl", "netgraph"],
    author='WebberTeam',
    author_email='admin@webberproject.com',
    url='https://www.webberproject.com',
    package_dir={'webber': 'webber'},
    package_data={'webber': ['viz/templates/*.html']},
    classifiers=['Development Status :: 3 - Alpha'],
)