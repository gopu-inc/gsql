from setuptools import setup, find_packages

setup(
    name="gsql",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "msgpack>=1.0.0",
        "rich>=13.0.0",
        "click>=8.0.0",
    ],
    entry_points={
        'console_scripts': [
            'gs=gsql.cli:main',
        ],
    },
    description="Simple yet powerful SQL database in Python",
    author="GSQL Contributors",
    license="MIT",
    python_requires=">=3.8",
)
