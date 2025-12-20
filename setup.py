# gsql/setup.py
from setuptools import setup, find_packages

setup(
    name="gsql-pure",
    version="1.0.0",
    description="Pure Python SQL Database - No External Dependencies",
    author="GOPU.inc",
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'gsql=gsql.__main__:main',
            'gs=gsql.__main__:main',
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
