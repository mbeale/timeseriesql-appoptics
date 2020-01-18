#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="timeseriesql-appoptics",
    version="0.1.0",
    description="A backend to query AppOptics data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="http://github.com/mbeale/timeseriesql-appoptics",
    author="Michael Beale",
    author_email="michael.beale@gmail.com",
    license="MIT",
    packages=["timeseriesql_appoptics"],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
    ],
    install_requires=["timeseriesql"],
    python_requires=">=3.6",
)

