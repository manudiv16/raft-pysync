from setuptools import setup
from raft_pysync import VERSION

description = "A library for replicating your python class between multiple servers, based on raft protocol"
try:
    import pypandoc

    long_description = pypandoc.convert_file("README.md", "rst")
except (IOError, ImportError, RuntimeError):
    long_description = description

setup(
    name="raft_pysync",
    packages=["raft_pysync"],
    version=VERSION,
    description=description,
    long_description=long_description,
    author="Manudiv16",
    license="MIT",
    url="https://github.com/manudiv16/raft-pysync",
    keywords=["network", "replication", "raft", "synchronization"],
    classifiers=[
        "Topic :: System :: Networking",
        "Topic :: System :: Distributed Computing",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS :: MacOS X",
        "License :: OSI Approved :: MIT License",
    ],
)
