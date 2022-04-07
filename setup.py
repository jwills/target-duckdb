#!/usr/bin/env python

from setuptools import setup

with open("README.md") as f:
    long_description = f.read()

setup(
    name="target-duckdb",
    version="0.1.0",
    description="Singer.io target for loading data into DuckDB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="TransferWise",
    url="https://github.com/jwills/target-duckdb",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
    ],
    py_modules=["target_duckdb"],
    install_requires=[
        "pipelinewise-singer-python>=1,<3",
        "duckdb==0.3.2",
        "inflection==0.5.1",
    ],
    extras_require={
        "test": [
            "pytest==7.1.1",
            "pylint==2.13.4",
            "pytest-cov==3.0.0",
        ]
    },
    entry_points="""
          [console_scripts]
          target-duckdb=target_duckdb:main
      """,
    packages=["target_duckdb"],
    package_data={},
    include_package_data=True,
)
