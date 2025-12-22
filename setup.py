#!/usr/bin/env python3
"""
Setup script for GSQL - SQL database engine with natural language interface
"""

import os
from pathlib import Path
from setuptools import setup, find_packages

# Read the long description from README.md
readme_path = Path(__file__).parent / "README.md"
long_description = ""
if readme_path.exists():
    with open(readme_path, "r", encoding="utf-8") as f:
        long_description = f.read()

# Get version from gsql/__init__.py
def get_version():
    init_path = Path(__file__).parent / "gsql" / "__init__.py"
    with open(init_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("__version__"):
                return line.split("=")[1].strip().strip('"\'')
    return "1.0.0"

# Ensure NLTK data is available
def download_nltk_data():
    """Download required NLTK data on installation"""
    try:
        import nltk
        required_data = [
            'punkt',
            'stopwords',
            'averaged_perceptron_tagger',
            'wordnet'
        ]
        
        for package in required_data:
            try:
                nltk.download(package, quiet=True)
                print(f"✓ NLTK data '{package}' downloaded")
            except Exception as e:
                print(f"⚠ Could not download NLTK data '{package}': {e}")
    except ImportError:
        print("⚠ NLTK not available, please install it manually")

# Post-installation script
def post_install():
    """Post-installation setup"""
    download_nltk_data()
    
    # Create default patterns file if it doesn't exist
    patterns_dir = Path("gsql/nlp")
    patterns_file = patterns_dir / "patterns.json"
    
    if not patterns_file.exists() and patterns_dir.exists():
        default_patterns = {
            "select_patterns": [
                {
                    "pattern": r"(montre|affiche|donne|sélectionne)\s+(.+?)\s+(de|depuis|from)\s+(\w+)",
                    "template": "SELECT {columns} FROM {table}"
                },
                {
                    "pattern": r"(combien|nombre|count)\s+(de|d')\s+(\w+)\s+(dans|in|from)\s+(\w+)",
                    "template": "SELECT COUNT(*) FROM {table}"
                },
                {
                    "pattern": r"liste\s+(?:les|tous les)\s+(\w+)",
                    "template": "SELECT * FROM {table}"
                }
            ],
            "where_patterns": [
                {
                    "pattern": r"(où|where)\s+(.+?)\s+(est|égale|=|>\s*|<\s*)(.+?)$",
                    "template": "WHERE {column} {operator} {value}"
                },
                {
                    "pattern": r"de\s+(\w+)\s+où\s+(.+?)\s+(est|>)",
                    "template": "FROM {table} WHERE {condition}"
                }
            ],
            "insert_patterns": [
                {
                    "pattern": r"(ajoute|insère)\s+(?:un|une)\s+(.+?)\s+(.+?)$",
                    "template": "INSERT INTO {table} VALUES ({values})"
                }
            ],
            "column_mapping": {
                "clients": ["nom", "email", "ville", "âge", "salaire", "date_inscription"],
                "produits": ["nom", "prix", "catégorie", "stock", "description"],
                "commandes": ["id", "client_id", "date", "montant", "statut", "produits"],
                "utilisateurs": ["id", "nom", "email", "mot_de_passe", "role", "actif"],
                "employés": ["id", "prénom", "nom", "département", "salaire", "date_embauche"]
            },
            "table_aliases": {
                "client": "clients",
                "produit": "produits",
                "commande": "commandes",
                "utilisateur": "utilisateurs",
                "employé": "employés",
                "employes": "employés",
                "user": "utilisateurs",
                "users": "utilisateurs"
            },
            "value_mapping": {
                "aujourd'hui": "CURRENT_DATE",
                "maintenant": "NOW()",
                "vrai": "TRUE",
                "faux": "FALSE",
                "null": "NULL"
            }
        }
        
        import json
        with open(patterns_file, "w", encoding="utf-8") as f:
            json.dump(default_patterns, f, ensure_ascii=False, indent=2)
        
        print("✓ Default NLP patterns created")

# Execute post-install
try:
    post_install()
except Exception as e:
    print(f"⚠ Post-installation setup incomplete: {e}")

setup(
    name="gsql",
    version=get_version(),
    author="Gopu Inc",
    author_email="contact@gopu-inc.com",
    description="A lightweight SQL database engine with natural language interface",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gopu-inc/gsql",
    
    # Package discovery
    packages=find_packages(include=['gsql', 'gsql.*']),
    package_dir={'': '.'},
    
    # Include package data
    package_data={
        'gsql': [
            'nlp/patterns.json',
            'nlp/*.json',
            '*.py',
            '*'
        ]
    },
    
    # Entry points for CLI
    entry_points={
        'console_scripts': [
            'gsql=gsql.__main__:main',
            'gs=gsql.__main__:main',
        ],
    },
    
    # Dependencies
    install_requires=[
        'nltk>=3.8.1',
        'colorama>=0.4.6',
        'tabulate>=0.9.0',
        'sqlparse>=0.4.4',
    ],
    
    # Optional dependencies
    extras_require={
        'dev': [
            'pytest>=7.4.0',
            'pytest-cov>=4.1.0',
            'black>=23.0.0',
            'flake8>=6.0.0',
            'mypy>=1.5.0',
        ],
        'nlp': [
            'spacy>=3.6.0',
            'transformers>=4.35.0',
            'torch>=2.0.0',
        ],
        'web': [
            'fastapi>=0.104.0',
            'uvicorn>=0.24.0',
            'jinja2>=3.1.0',
        ],
    },
    
    # Python version requirements
    python_requires='>=3.8',
    
    # Metadata
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Database :: Database Engines/Servers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    
    # Keywords
    keywords='sql database nlp natural-language sqlite',
    
    # Project URLs
    project_urls={
        'Bug Reports': 'https://github.com/gopu-inc/gsql/issues',
        'Source': 'https://github.com/gopu-inc/gsql',
        'Documentation': 'https://github.com/gopu-inc/gsql/wiki',
    },
    
    # License
    license='MIT',
    
    # Include data files
    include_package_data=True,
    zip_safe=False,
)
