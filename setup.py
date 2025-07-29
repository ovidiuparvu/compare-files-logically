"""
Setup script for compare-files-logically package.
"""

from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="compare-files-logically",
    version="0.1.0",
    author="JetBrains",
    author_email="info@jetbrains.com",
    description=(
        "A Python library for comparing files logically, "
        "with special handling for CSV and parquet files"
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jetbrains/compare-files-logically",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities",
    ],
    python_requires=">=3.6",
    install_requires=[],
    extras_require={
        "parquet": ["polars>=0.20.0"],
    },
)