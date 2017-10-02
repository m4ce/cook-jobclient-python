import os
import pip
from pip.req import parse_requirements
from setuptools import setup, find_packages
from cook import __version__ as version

requirements = parse_requirements(
    'requirements.txt', session=pip.download.PipSession())
reqs = [str(ir.req) for ir in requirements]

with open(os.path.join(os.getcwd(), 'README.md')) as f:
    readme = f.read()

setup(
    name='cook-jobclient',
    version=version,
    description='Python library for Cook scheduler REST API',
    long_description=readme,
    author='Matteo Cerutti',
    author_email='matteo.cerutti@hotmail.co.uk',
    url='https://github.com/m4ce/cook-jobclient-python',
    license='Apache License 2.0',
    packages=find_packages(exclude=['tests', 'etc', 'examples']),
    include_package_data=True,
    keywords=[
        'cook',
        'mesos',
        'jobclient',
    ],
    setup_requires=[
        'pytest-runner'
    ],
    tests_require=[
        'mock',
        'pytest',
        'pytest-cov'
    ],
    install_requires=reqs
)
