#!/usr/bin/env python
# Copyright (c) 2021 dciangot
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT


from __future__ import print_function

import os
import sys

from setuptools import setup
from glob import glob

pjoin = os.path.join
here = os.path.abspath(os.path.dirname(__file__))

# Get the current package version.
version_ns = {}
with open(pjoin(here, "version.py")) as f:
    exec(f.read(), {}, version_ns)

with open(pjoin(here, "README.md"), encoding="utf-8") as f:
    long_desc = f.read()

setup_args = dict(
    name="dask-remote-jobqueue",
    packages=["dask_remote_jobqueue"],
    version=version_ns["__version__"],
    description="""Remote jobqueu implementation for DASK: create a scheduler on a remote job queue cluster""",
    long_description=long_desc,
    long_description_content_type="text/markdown",
    author="D. Ciangottini, D. Spiga, M. Tracolli,",
    author_email="dciangot@cern.ch",
    url="http://github.com/dodas-ts/dask-remote-jobqueue",
    license="MIT",
    platforms="Linux",
    python_requires="~=3.7",
    keywords=["Interactive", "Dask", "Distributed"],
)

# setuptools requirements
if "setuptools" in sys.modules:
    setup_args["install_requires"] = install_requires = []
    with open("requirements.txt") as f:
        for line in f.readlines():
            req = line.strip()
            if not req or req.startswith(("-e", "#")):
                continue
            install_requires.append(req)


def main():
    setup(**setup_args)


if __name__ == "__main__":
    main()
