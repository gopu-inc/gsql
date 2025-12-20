# setup.py
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="gsql-pure",
    version="1.1.0",
    author="GOPU.inc",
    author_email="",
    description="Simple yet powerful SQL database - 100% Pure Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gopu-inc/gsql",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=[
        "tomli>=2.0.0; python_version < '3.11'",
    ],
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'black>=23.0.0',
        ]
    },
    entry_points={
        'console_scripts': [
            'gsql=gsql.__main__:main',
            'gs=gsql.__main__:main',
        ],
    },
    keywords="sql database db embedded",
)
