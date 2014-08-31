#!/usr/bin/env python

from setuptools import setup
from ntfs import __version__


setup(name="python-ntfs",
        version=__version__,
        description="Open source Python library for NTFS analysis",
        author="Willi Ballenthin",
        author_email="willi.ballenthin@gmail.com",
        url="http://www.williballenthin.com/forensics/ntfs",
        license="Apache License (2.0)",
        packages=[
            "ntfs",
            "ntfs.mft",
            #"nfts.secure",
            #"ntfs.logfile",
            #"ntfs.usnjrnl",
            ],
        classifiers=["Programming Language :: Python",
            "Operating System :: OS Independent",
            "License :: OSI Approved :: Apache Software License"],
        install_requires=["enum34"])
