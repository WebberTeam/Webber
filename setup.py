from setuptools import setup, find_packages
setup(
    name='webber',
    version='0.3.0',
    description='A Pythonic platform for no-latency ETL and embarrassingly parallel directed acyclic graphs.',
    long_description='A Pythonic platform for no-latency ETL and embarrassingly parallel directed acyclic graphs.',
    long_description_content_type='text/x-rst',
    packages=find_packages(include=['webber', 'webber.*']),
    install_requires=["networkx", "flask", "pyvis", "jinja2", "matplotlib", "ipympl", "netgraph"],
    author='WebberTeam',
    author_email='admin@webberproject.com',
    url='https://www.webberproject.com',
    package_dir={'webber': 'webber'},
    package_data={'webber': ['viz/templates/*.html']},
    classifiers=['Development Status :: 4 - Beta'],
)