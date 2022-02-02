from setuptools import setup

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="better_adf",
    version="0.0.1",
    description="",
    author="Daniel van der Ende",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=["better_adf"],
    install_requires=[
        "azure-mgmt-resource==20.1.0",
        "azure-mgmt-datafactory==2.2.0",
    ],
    extras_require={
        "deploy": ["azure-identity==1.7.1"],
        "test": ["pytest==6.2.5"],
        "lint": ["flake8==4.0.1"],
    },
)
