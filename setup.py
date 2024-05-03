#!/usr/bin/env python

from setuptools import setup

with open("README.md") as f:
    long_description = f.read()

setup(
    name="target-duckdb",
    version="0.6.0",
    description="Singer.io target for loading data into DuckDB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Josh Wills",
    url="https://github.com/jwills/target-duckdb",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
    ],
    py_modules=["target_duckdb"],
    install_requires=[
        "jsonschema>=3.2.0",
        "duckdb==0.9.2",
    ],
    extras_require={
        "test": [
            "pytest",
            "pylint",
            "pytest-cov",
        ]
    },
    entry_points="""
          [console_scripts]
          target-duckdb=target_duckdb:main
      """,
    packages=["target_duckdb"],
    package_data={"target_duckdb": ["logging.conf"]},
    include_package_data=True,
)
