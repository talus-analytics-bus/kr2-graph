from setuptools import setup

setup(
    name = "ncbi",
    version = "0.0.1",
    author = "Talus Analytics LLC",
    description = ("NCBI application programming interface"),
    install_requires=[
        "requests",
        "beautifulsoup4",
        "loguru",
    ],
    packages=['ncbi'],
    classifiers=[
        "Development Status :: 3 - Alpha",
    ],
)