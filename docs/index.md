# Welcome to 🏭🍰 adfPy

AdfPy is an opinionated, Pythonic wrapper around the Azure Data Factory (ADF) SDK. It aims to provide Airflow-like syntax for defining data pipelines in ADF, making it easier and more reliable to build data pipelines in a robust manner. 

The documentation here is intended to make easy and clear how to use AdfPy. Please also check out the [examples](https://github.com/danielvdende/adfpy/tree/main/examples) included in the repository.

## Key features and principles
- adfPy does *not* aim to be 100% feature-complete (at least, not for now). Instead, it exposes the most-used components of ADF. If you do miss something, don't hesitate to contribute! We're more than happy to receive any help.
- Where possible, we have stuck with a naming convention of prefixing the ADF SDK function/class name with `Adf`. This makes it clear what part of the SDK we are wrapping, but also ensure it's clear what components are part of adfPy.

## Contributing
We're more than happy to receive any contributions: big, small, or anywhere in between! Please open an issue and a PR on github and we'll get back to you as soon as we can!