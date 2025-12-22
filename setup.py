"""
Setup script for GSQL - Complete SQL Database System in Python
Version 3.0.0
"""

from setuptools import setup, find_packages
import os
import re

# Read version from __init__.py
def get_version():
    """Extract version from gsql/__init__.py"""
    init_path = os.path.join(os.path.dirname(__file__), 'gsql', '__init__.py')
    with open(init_path, 'r', encoding='utf-8') as f:
        content = f.read()
        match = re.search(r'__version__\s*=\s*[\'"]([^\'"]+)[\'"]', content)
        if match:
            return match.group(1)
    return '3.0.0'

# Read long description from README
def get_long_description():
    """Read long description from README.md"""
    try:
        with open('README.md', 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        return "GSQL - Complete SQL Database System in Python"

# Get dependencies from requirements.txt
def get_requirements():
    """Read requirements from requirements.txt"""
    requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(requirements_path):
        with open(requirements_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

# Get development dependencies
def get_dev_requirements():
    """Read development requirements from requirements-dev.txt"""
    dev_requirements_path = os.path.join(os.path.dirname(__file__), 'requirements-dev.txt')
    if os.path.exists(dev_requirements_path):
        with open(dev_requirements_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

setup(
    # Basic Information
    name="gsql",
    version=get_version(),
    description="Complete SQL Database System in Python with AI Integration",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    
    # Authors
    author="Gopu Inc.",
    author_email="contact@gopu-inc.com",
    
    # Project URLs
    url="https://github.com/gopu-inc/gsql",
    project_urls={
        "Homepage": "https://github.com/gopu-inc/gsql",
        "Documentation": "https://gsql.readthedocs.io",
        "Source Code": "https://github.com/gopu-inc/gsql",
        "Bug Tracker": "https://github.com/gopu-inc/gsql/issues",
        "Changelog": "https://github.com/gopu-inc/gsql/releases",
    },
    
    # Classifiers
    classifiers=[
        # Development Status
        "Development Status :: 5 - Production/Stable",
        
        # Intended Audience
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        
        # Topics
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Systems Administration",
        "Topic :: Scientific/Engineering :: Information Analysis",
        
        # License
        "License :: OSI Approved :: MIT License",
        
        # Programming Languages
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        
        # Operating Systems
        "Operating System :: OS Independent",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Operating System :: Unix",
        "Operating System :: MacOS",
        
        # Additional
        "Environment :: Console",
        "Natural Language :: English",
        "Natural Language :: French",
    ],
    
    # Keywords
    keywords=[
        "sql",
        "database",
        "sqlite",
        "python",
        "ai",
        "nlp",
        "query",
        "relational",
        "data",
        "analytics",
        "management",
        "server",
        "cli",
        "shell",
        "indexing",
        "storage",
        "transaction",
    ],
    
    # Packages
    packages=find_packages(
        include=['gsql', 'gsql.*'],
        exclude=['tests', 'tests.*', 'docs', 'docs.*', 'examples', 'examples.*']
    ),
    
    # Include package data
    package_data={
        'gsql': [
            'nlp/data/*.json',
            'nlp/models/*.pkl',
            'config/*.yaml',
            'config/*.json',
        ],
    },
    
    # Exclude certain files
    exclude_package_data={
        '': [
            '*.pyc',
            '*.pyo',
            '__pycache__',
            '*.so',
            '*.dll',
            '*.pyd',
        ],
    },
    
    # Dependencies
    install_requires=get_requirements(),
    
    # Development dependencies
    extras_require={
        'dev': get_dev_requirements(),
        'test': [
            'pytest>=7.0.0',
            'pytest-cov>=4.0.0',
            'pytest-mock>=3.10.0',
            'pytest-asyncio>=0.21.0',
        ],
        'docs': [
            'sphinx>=7.0.0',
            'sphinx-rtd-theme>=1.3.0',
            'sphinx-autodoc-typehints>=1.24.0',
        ],
        'performance': [
            'pandas>=1.5.0',
            'numpy>=1.24.0',
            'psutil>=5.9.0',
        ],
        'nlp': [
            'nltk>=3.8.0',
            'spacy>=3.6.0',
            'transformers>=4.30.0',
            'torch>=2.0.0',
        ],
        'full': [
            'pandas>=1.5.0',
            'numpy>=1.24.0',
            'nltk>=3.8.0',
            'spacy>=3.6.0',
            'transformers>=4.30.0',
            'torch>=2.0.0',
            'psutil>=5.9.0',
            'prompt-toolkit>=3.0.0',
            'click>=8.0.0',
            'colorama>=0.4.0',
        ],
    },
    
    # Python version requirements
    python_requires=">=3.8",
    
    # Entry points
    entry_points={
        'console_scripts': [
            'gsql=gsql.__main__:main',
            'gsql-cli=gsql.cli.shell:main',
            'gsql-server=gsql.api.rest_api:main',
        ],
    },
    
    # Zip safe
    zip_safe=False,
    
    # Platforms
    platforms=["any"],
    
    # License
    license="MIT License",
    
    # Download URL
    download_url="https://github.com/gopu-inc/gsql/archive/refs/tags/v3.0.0.tar.gz",
    
    # Additional metadata
    provides=["gsql"],
    
    # Options for building
    options={
        'bdist_wheel': {
            'universal': False,
        },
        'egg_info': {
            'tag_build': '',
            'tag_date': False,
        },
    },
    
    # Scripts
    scripts=[
        'scripts/gsql-backup',
        'scripts/gsql-migrate',
        'scripts/gsql-monitor',
    ] if os.path.exists('scripts') else [],
)

# Additional setup for building
if __name__ == "__main__":
    print(f"Building GSQL version {get_version()}")
