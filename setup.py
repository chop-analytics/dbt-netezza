#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt-netezza"
package_version = "0.6.0"
description = """The netezza adpter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="Joe Mirizio",
    author_email="mirizioj@chop.edu",
    url="analytics.chop.edu",
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/netezza/dbt_project.yml',
            'include/netezza/macros/*.sql',
            'include/netezza/macros/**/**/*.sql',
            "include/netezza/macros/**/**/**/*.sql"
        ]
    },
    install_requires=[
        'dbt-core~=1.0',
        'pyodbc'
    ]
)
