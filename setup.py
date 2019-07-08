#! /usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="weeds_py",
    version="0.1",
    author="Tom Rice",
    author_email="tsrice@umich.edu",
#    description=("Analyze and maniuplate FITS cubes of protoplanetary disks."),
#    long_description=long_description,
#    long_description_content_type="text/markdown",
#    url="https://github.com/richteague/imgcube",
    packages=["weeds_py"],
    license="MIT",
    install_requires=["scipy", "numpy", "matplotlib", "astropy"],
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
