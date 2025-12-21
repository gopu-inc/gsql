from setuptools import setup, find_packages

setup(
    name="gsql",
    version="1.0.0",
    description="Simple yet powerful SQL database - 100% Pure Python",
    author="GSQL Team",
    packages=find_packages(),
    python_requires=">=3.6",
    install_requires=[],
    entry_points={
        'console_scripts': [
            'gsql=gsql.__main__:main',
        ],
    },
)
