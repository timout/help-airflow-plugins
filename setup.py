
from setuptools import find_packages
from setuptools import setup

from glob import glob
from os.path import basename
from os.path import splitext

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='help-airflow-plugins',
    version='0.1',
    description="Set of small airflow plugins",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages('src/main/python'),
    package_dir={'': 'src/main/python'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/main/python/*.py')],
    test_suite='src/test/python',
    url='https://github.com/timout/help-airflow-plugins.git',
    author='timout'
)
