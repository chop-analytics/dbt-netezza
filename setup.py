#!/usr/bin/env python
from setuptools import find_namespace_packages, setup

package_name = "dbt-netezza"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "1.1.0"
description = """The Netezza adapter plugin for dbt"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Joe Mirizio",
    author_email="mirizioj@chop.edu",
    url="https://github.com/chop-analytics/dbt-netezza",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        'dbt-core~=1.2.0',
        'pyodbc~=4.0'
    ]
)
