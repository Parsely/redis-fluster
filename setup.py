#!/usr/bin/env python
__license__ = """
Copyright 2015 Parse.ly, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import re

from setuptools import setup, find_packages

# Get version without importing, which avoids dependency issues
def get_version():
    with open("fluster/__init__.py") as version_file:
        return re.search(
            r"""__version__\s+=\s+(['"])(?P<version>.+?)\1""", version_file.read()
        ).group("version")


def readme():
    """ Returns README.rst contents as str """
    with open("README.rst") as f:
        return f.read()


install_requires = ["mmh3", "redis", "hiredis"]
tests_require = ["mock", "pytest", "testinstances"]
setup_requires = ["pytest-runner"]

setup(
    name="fluster",
    version=get_version(),
    author="Keith Bourgoin",
    author_email="hello@parsely.com",
    url="https://github.com/Parsely/redis-fluster",
    description="Redis Cluster with Some Features",
    long_description=readme(),
    keywords="redis cluster",
    license="Apache License 2.0",
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=setup_requires,
    zip_safe=True,
    include_package_data=True,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
