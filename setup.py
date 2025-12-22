"""
Setup script for GSQL - Complete SQL Database System in Python
Version 3.0.1+
"""

from setuptools import setup, find_packages
from setuptools.command.build_py import build_py
from setuptools.command.egg_info import egg_info
import os
import sys
import glob
import re

class VerifyManifestCommand(build_py):
    """Vérifie que tous les fichiers du MANIFEST.in existent avant la construction"""
    description = "Vérifie les fichiers du MANIFEST.in avant la construction"
    
    def run(self):
        print("🔍 Vérification des fichiers dans MANIFEST.in...")
        
        # Lire le MANIFEST.in
        manifest_path = os.path.join(os.path.dirname(__file__), 'MANIFEST.in')
        if not os.path.exists(manifest_path):
            print("⚠️  MANIFEST.in non trouvé, création d'un MANIFEST.in par défaut...")
            self._create_default_manifest()
        
        with open(manifest_path, 'r', encoding='utf-8') as f:
            manifest_content = f.read()
        
        # Extraire les patterns de fichiers
        include_patterns = []
        recursive_patterns = []
        
        for line in manifest_content.split('\n'):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            if line.startswith('include '):
                pattern = line[8:].strip()
                include_patterns.append(pattern)
            elif line.startswith('recursive-include '):
                parts = line[18:].split()
                if len(parts) >= 2:
                    directory = parts[0]
                    pattern = parts[1]
                    recursive_patterns.append((directory, pattern))
            elif line.startswith('global-exclude'):
                continue  # Ignorer les exclusions
        
        # Vérifier les fichiers
        missing_files = []
        existing_files = []
        
        # Vérifier les include simples
        for pattern in include_patterns:
            files = glob.glob(pattern, recursive=True)
            if not files:
                missing_files.append(pattern)
            else:
                existing_files.extend(files)
        
        # Vérifier les recursive-include
        for directory, pattern in recursive_patterns:
            full_pattern = os.path.join(directory, '**', pattern)
            files = glob.glob(full_pattern, recursive=True)
            if not files:
                # Essayer sans le ** pour la compatibilité
                alt_pattern = os.path.join(directory, pattern)
                files = glob.glob(alt_pattern, recursive=True)
            
            if not files:
                missing_files.append(f"{directory}/**/{pattern}")
            else:
                existing_files.extend(files)
        
        # Afficher les résultats
        if missing_files:
            print(f"❌ {len(missing_files)} fichiers/patterns manquants:")
            for pattern in missing_files:
                print(f"   - {pattern}")
            print("\n💡 Solutions possibles:")
            print("   1. Créez les fichiers manquants")
            print("   2. Mettez à jour le MANIFEST.in")
            print("   3. Ignorez avec '--force' pour continuer")
            
            if '--force' not in sys.argv:
                response = input("\n❓ Continuer malgré les fichiers manquants? (o/N): ")
                if response.lower() != 'o':
                    print("❌ Construction annulée.")
                    sys.exit(1)
        else:
            print(f"✅ Tous les fichiers trouvés ({len(existing_files)} fichiers)")
        
        # Continuer avec la construction normale
        super().run()
    
    def _create_default_manifest(self):
        """Crée un MANIFEST.in par défaut"""
        manifest_content = """# MANIFEST.in pour GSQL
# Fichiers inclus dans la distribution

# Fichiers de base
include LICENSE
include README.md
include CHANGELOG.md
include CONTRIBUTING.md
include requirements.txt
include requirements-dev.txt
include pyproject.toml
include setup.py
include MANIFEST.in

# Package principal
recursive-include gsql *.py
recursive-include gsql *.json
recursive-include gsql *.yaml
recursive-include gsql *.txt
recursive-include gsql *.sql

# Exclusions
global-exclude *.pyc
global-exclude *.pyo
global-exclude __pycache__
global-exclude *.so
global-exclude *.dylib
global-exclude *.dll
global-exclude *.pyd
"""
        
        with open('MANIFEST.in', 'w', encoding='utf-8') as f:
            f.write(manifest_content)
        print("✅ MANIFEST.in par défaut créé")

class CustomEggInfoCommand(egg_info):
    """Commande egg_info personnalisée qui vérifie aussi le MANIFEST.in"""
    def run(self):
        # D'abord vérifier le MANIFEST.in
        cmd = VerifyManifestCommand(self.distribution)
        cmd.run()
        # Puis exécuter la commande normale
        super().run()

def get_version():
    """Extract version from gsql/__init__.py avec fallback"""
    try:
        init_path = os.path.join(os.path.dirname(__file__), 'gsql', '__init__.py')
        with open(init_path, 'r', encoding='utf-8') as f:
            content = f.read()
            match = re.search(r'__version__\s*=\s*[\'"]([^\'"]+)[\'"]', content)
            if match:
                return match.group(1)
    except:
        pass
    
    # Fallback: utiliser setuptools_scm si disponible
    try:
        from setuptools_scm import get_version as scm_get_version
        return scm_get_version()
    except:
        return '3.0.1'

def get_long_description():
    """Read long description from README.md avec fallback"""
    try:
        with open('README.md', 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        # Vérifier si README.md est dans MANIFEST.in
        manifest_path = os.path.join(os.path.dirname(__file__), 'MANIFEST.in')
        if os.path.exists(manifest_path):
            with open(manifest_path, 'r', encoding='utf-8') as f:
                if 'README.md' in f.read():
                    print("⚠️  README.md listé dans MANIFEST.in mais fichier non trouvé")
        
        return """# GSQL - Complete SQL Database System in Python

GSQL is a complete SQL database system written in Python with AI integration,
NLP capabilities, and multi-backend storage support.

## Features
- Full SQL engine with ACID transactions
- AI-powered NLP to SQL translation
- Multi-backend storage (SQLite, Memory, YAML)
- Intelligent caching and indexing
- Interactive CLI with auto-completion
- REST/GraphQL API support
- Docker and Kubernetes ready

## Quick Start
```bash
pip install gsql
gsql --help"""
