![tests](https://github.com/danielvdende/adfpy/actions/workflows/lint_test.yml/badge.svg)
# 🏭🍰 adfPy
adfPy aims to make developers lives easier by wrapping the Azure Data Factory Python SDK with an intuitive, powerful, and easy to use API that hopefully will remind people of working with Apache Airflow ;-). 

## Install
```shell
pip install adfpy
```

## Usage
Generally, using adfPy has 2 main components:
1. Write your pipeline.
2. Deploy your pipeline.

adfPy has an opinionated syntax, which is heavily influenced by Airflow. For documentation on what the syntax looks like, please read the docs [here](https://danielvdende.github.io/adfpy/).
Some examples are provided in the examples directory of this repository.


Once you've written your pipelines, it's time to deploy them! For this, you can use adfPy's deployment script:
```shell
pip install adfpy
adfpy-deploy --path <your_path_here>
```
Note:
- This script will ensure all pipelines in the provided path are present in your target ADF.
- This script will also **remove** any ADF pipelines that are **not** in your path, but are in ADF.

## Still to come
adfPy is still in development. As such, some ADF components are not yet supported:
- Datasets
- Linked services
- Triggers (support for Schedule Triggers is available, but not for Tumbling Window, Custom Event, or Storage Event)

## Developer setup
adfPy is built with [Poetry](https://python-poetry.org/). To setup a development environment run:
```shell
poetry install
```
