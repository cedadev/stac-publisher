""" Setup for the STAC Publisher. """

__author__ = "Rhys Evans"
__date__ = "2022-09-07"
__copyright__ = "Copyright 2019 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level directory"

from setuptools import find_packages, setup

# Get the long description from the relevant file
with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="stac-publisher",
    version="0.1.0",
    description="STAC Publisher",
    long_description=long_description,
    long_description_content_type="text/markdown",
    include_package_data=True,
    url="https://github.com/cedadev/stac-publisher",
    author="Rhys Evans",
    author_email="rhys.r.evans@stfc.ac.uk",
    license="BSD",
    classifiers=[
        "Development Status :: 2 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages(),
    install_requires=[
        "elasticsearch",
        "elasticsearch-dsl",
        "pika",
        "PyYAML",
    ],
)
