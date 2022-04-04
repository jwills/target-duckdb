#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name="target-duckdb",
      version="0.1.0",
      description="Singer.io target for loading data to DuckDB",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="TransferWise",
      url='https://github.com/jwills/target-duckdb',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=["target_duckdb"],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'duckdb==0.3.2',
          'inflection==0.5.1',
      ],
      extras_require={
          "test": [
              'pytest==6.2.1',
              'pylint==2.6.0',
              'pytest-cov==2.10.1',
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
