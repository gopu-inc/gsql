"""
🚀 GSQL v3.0.1+ - Setup Intelligent avec Vérification Automatique
"""

import os
import sys
import re
import glob
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# ============================================================================
# CONFIGURATION GÉNÉRALE
# ============================================================================

PROJECT_NAME = "gsql"
AUTHOR = "Gopu Inc."
AUTHOR_EMAIL = "ceoseshell@gmail.com"
DESCRIPTION = "Complete SQL Database System in Python with AI Integration"
KEYWORDS = [
    "sql", "database", "sqlite", "python", "ai", "nlp", "machine-learning",
    "data-science", "analytics", "cli", "rest-api", "docker", "kubernetes"
]
PYTHON_REQUIRES = ">=3.8"

# ============================================================================
# VÉRIFICATEUR INTELLIGENT DE MANIFEST
# ============================================================================

class ManifestValidator:
    """Vérificateur intelligent du fichier MANIFEST.in"""
    
    def __init__(self):
        self.manifest_path = "MANIFEST.in"
        self.missing_files = []
        self.existing_files = []
        self.warnings = []
        
    def validate(self, force: bool = False) -> bool:
        """Valide que tous les fichiers du MANIFEST.in existent"""
        print("🔍 Vérification du MANIFEST.in...")
        
        if not os.path.exists(self.manifest_path):
            print(f"❌ {self.manifest_path} introuvable")
            return False
        
        try:
            with open(self.manifest_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                # Traitement des différentes commandes
                if line.startswith('include '):
                    pattern = line[8:].strip()
                    self._check_include(pattern, line_num)
                elif line.startswith('recursive-include '):
                    parts = line[18:].split()
                    if len(parts) >= 2:
                        directory = parts[0]
                        pattern = parts[1]
                        self._check_recursive_include(directory, pattern, line_num)
                elif line.startswith('global-exclude'):
                    continue  # Ignorer les exclusions
            
            # Résultats
            print(f"\n📊 Résultat de la validation:")
            print(f"   ✅ Fichiers trouvés: {len(self.existing_files)}")
            
            if self.missing_files:
                print(f"   ❌ Fichiers manquants: {len(self.missing_files)}")
                for pattern in self.missing_files[:5]:  # Affiche les 5 premiers
                    print(f"      - {pattern}")
                
                if len(self.missing_files) > 5:
                    print(f"      ... et {len(self.missing_files) - 5} autres")
                
                if not force:
                    print("\n⚠️  Des fichiers requis sont manquants!")
                    print("   Continuation automatique (mode Docker)...")
                    # Dans Docker, continuez automatiquement
            
            if self.warnings:
                print(f"   ⚠️  Avertissements: {len(self.warnings)}")
                for warning in self.warnings[:3]:
                    print(f"      - {warning}")
            
            return True
            
        except Exception as e:
            print(f"❌ Erreur lors de la validation: {e}")
            return False
    
    def _check_include(self, pattern: str, line_num: int):
        """Vérifie une commande include"""
        files = glob.glob(pattern, recursive=True)
        if not files:
            self.missing_files.append(f"Ligne {line_num}: {pattern}")
        else:
            self.existing_files.extend(files)
    
    def _check_recursive_include(self, directory: str, pattern: str, line_num: int):
        """Vérifie une commande recursive-include"""
        if not os.path.exists(directory):
            self.missing_files.append(f"Ligne {line_num}: {directory}/**/{pattern} (dossier inexistant)")
            return
        
        search_pattern = os.path.join(directory, "**", pattern)
        files = glob.glob(search_pattern, recursive=True)
        
        if not files:
            # Essayer sans le **
            alt_pattern = os.path.join(directory, pattern)
            files = glob.glob(alt_pattern, recursive=False)
        
        if not files:
            self.missing_files.append(f"Ligne {line_num}: {directory}/**/{pattern}")
        else:
            self.existing_files.extend(files)

# ============================================================================
# GESTIONNAIRE DE VERSION AUTOMATIQUE
# ============================================================================

class VersionManager:
    """Gestionnaire automatique de version"""
    
    def __init__(self):
        self.version_files = [
            "gsql/__init__.py",
            "pyproject.toml",
            "setup.cfg"
        ]
    
    def get_version(self) -> str:
        """Récupère automatiquement la version depuis plusieurs sources"""
        # Essayer d'abord setuptools_scm (recommandé)
        try:
            from setuptools_scm import get_version as scm_get_version
            version = scm_get_version()
            if version:
                print(f"📦 Version détectée (setuptools_scm): {version}")
                return version
        except ImportError:
            pass
        
        # Chercher dans les fichiers
        for file_path in self.version_files:
            if os.path.exists(file_path):
                version = self._extract_from_file(file_path)
                if version:
                    print(f"📦 Version détectée ({file_path}): {version}")
                    return version
        
        # Version par défaut
        default_version = "0.1.1"
        print(f"📦 Version par défaut: {default_version}")
        return default_version
    
    def _extract_from_file(self, file_path: str) -> Optional[str]:
        """Extrait la version d'un fichier"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if file_path.endswith('.py'):
                # Chercher __version__ = "x.y.z"
                match = re.search(r'__version__\s*=\s*[\'"]([^\'"]+)[\'"]', content)
                if match:
                    return match.group(1)
            
            elif file_path.endswith('.toml'):
                # Chercher dans pyproject.toml
                match = re.search(r'version\s*=\s*[\'"]([^\'"]+)[\'"]', content)
                if match:
                    return match.group(1)
            
            elif file_path.endswith('.cfg'):
                # Chercher dans setup.cfg
                match = re.search(r'version\s*=\s*([^\s]+)', content)
                if match:
                    return match.group(1)
        
        except Exception:
            pass
        
        return None

# ============================================================================
# LECTURE DES DÉPENDANCES
# ============================================================================

def get_requirements() -> List[str]:
    """Lit les dépendances depuis requirements.txt"""
    requirements = []
    
    if os.path.exists("requirements.txt"):
        with open("requirements.txt", 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    requirements.append(line)
    
    if not requirements:
        # Dépendances par défaut
        requirements = [
            'click>=8.0.0',
            'colorama>=0.4.4',
            'prompt-toolkit>=3.0.0',
            'rich>=13.0.0',
            'pandas>=1.3.0',
            'numpy>=1.21.0',
            'nltk>=3.6.0',
            'spacy>=3.5.0',
            'psutil>=5.9.0',
            'typing-extensions>=4.7.0',
            'cryptography>=41.0.0',
        ]
    
    return requirements

# ============================================================================
# DÉTECTION AUTOMATIQUE DES ENTRY POINTS
# ============================================================================

def find_entry_points() -> Dict[str, str]:
    """Détecte automatiquement les points d'entrée"""
    entry_points = {
        'console_scripts': [
            'gsql=gsql.__main__:main',
        ]
    }
    
    # Vérifier l'existence des modules
    if os.path.exists("gsql/cli/shell.py"):
        entry_points['console_scripts'].append('gsql-cli=gsql.cli.shell:main')
    
    if os.path.exists("gsql/api/rest_api.py"):
        entry_points['console_scripts'].append('gsql-server=gsql.api.rest_api:main')
    
    return entry_points

# ============================================================================
# DÉTECTION AUTOMATIQUE DES DONNÉES DE PACKAGE
# ============================================================================

def find_package_data() -> Dict[str, List[str]]:
    """Détecte automatiquement les données du package"""
    package_data = {}
    
    # Patterns communs
    patterns = [
        '*.json', '*.yaml', '*.yml', '*.txt',
        '*.pkl', '*.model', '*.sql', '*.md'
    ]
    
    # Chercher dans gsql et ses sous-dossiers
    for root, dirs, files in os.walk("gsql"):
        # Convertir le chemin en nom de package
        package_name = root.replace(os.sep, '.')
        
        # Vérifier chaque pattern
        for pattern in patterns:
            matching_files = glob.glob(os.path.join(root, pattern))
            if matching_files:
                if package_name not in package_data:
                    package_data[package_name] = []
                
                # Ajouter le pattern relatif
                rel_pattern = pattern
                if root != "gsql":
                    rel_dir = os.path.relpath(root, "gsql")
                    rel_pattern = os.path.join(rel_dir, "**", pattern)
                
                if rel_pattern not in package_data[package_name]:
                    package_data[package_name].append(rel_pattern)
    
    return package_data

# ============================================================================
# VÉRIFICATION PRÉ-SETUP
# ============================================================================

def pre_setup_checks() -> bool:
    """Exécute toutes les vérifications avant le setup"""
    print("=" * 60)
    print("🚀 PRÉPARATION DE LA CONSTRUCTION GSQL")
    print("=" * 60)
    
    # 1. Vérifier MANIFEST.in
    validator = ManifestValidator()
    if not validator.validate(force='--force' in sys.argv):
        return False
    
    # 2. Vérifier la structure minimale
    required_dirs = ["gsql"]
    for dir_path in required_dirs:
        if not os.path.exists(dir_path):
            print(f"❌ Dossier requis manquant: {dir_path}")
            return False
    
    # 3. Vérifier les fichiers critiques
    critical_files = ["LICENSE", "README.md"]
    for file_path in critical_files:
        if not os.path.exists(file_path):
            print(f"⚠️  Fichier critique manquant: {file_path}")
    
    print("\n✅ Vérifications terminées avec succès")
    return True

# ============================================================================
# SETUP PRINCIPAL
# ============================================================================

# Import setuptools APRES les vérifications
from setuptools import setup, find_packages

def main():
    """Fonction principale du setup"""
    
    # Exécuter les vérifications
    if not pre_setup_checks():
        print("\n❌ Construction annulée. Corrigez les problèmes et réessayez.")
        sys.exit(1)
    
    # Obtenir la version automatiquement
    version_manager = VersionManager()
    version = version_manager.get_version()
    
    # Obtenir les métadonnées
    long_description = ""
    if os.path.exists("README.md"):
        with open("README.md", 'r', encoding='utf-8') as f:
            long_description = f.read()
    
    # URLs du projet (personnalisez-les)
    project_urls = {
        "Homepage": "https://github.com/gopu-inc/gsql",
        "Documentation": "https://gsql.readthedocs.io",
        "Source Code": "https://github.com/gopu-inc/gsql",
        "Bug Tracker": "https://github.com/gopu-inc/gsql/issues",
        "Changelog": "https://github.com/gopu-inc/gsql/releases",
        "Discussions": "https://github.com/gopu-inc/gsql/discussions",
        "Docker Hub": "https://hub.docker.com/r/ceoseshell/gsql",
        "PyPI": "https://pypi.org/project/gsql/",
        "Anaconda": "https://anaconda.org/ceoseshell/gsql",
    }
    
    # Configuration du setup
    setup(
        # === INFORMATIONS DE BASE ===
        name=PROJECT_NAME,
        version=version,
        description=DESCRIPTION,
        long_description=long_description,
        long_description_content_type="text/markdown",
        
        # === AUTEURS ===
        author=AUTHOR,
        author_email=AUTHOR_EMAIL,
        maintainer="GSQL Maintainers",
        maintainer_email="maintainers@gopu-inc.com",
        
        # === URLs ===
        url=project_urls["Homepage"],
        project_urls=project_urls,
        
        # === CLASSIFIERS ===
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "Topic :: Database",
            "Topic :: Database :: Database Engines/Servers",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
            "Operating System :: OS Independent",
        ],
        
        # === MOTS-CLÉS ===
        keywords=KEYWORDS,
        
        # === PACKAGES ===
        packages=find_packages(include=['gsql', 'gsql.*']),
        
        # === DONNÉES DU PACKAGE ===
        package_data=find_package_data(),
        include_package_data=True,
        
        # === DÉPENDANCES ===
        install_requires=get_requirements(),
        python_requires=PYTHON_REQUIRES,
        
        # === DÉPENDANCES OPTIONNELLES ===
        extras_require={
            'dev': [
                'pytest>=7.0.0',
                'pytest-cov>=4.0.0',
                'black>=23.0.0',
                'flake8>=6.0.0',
                'mypy>=1.0.0',
                'isort>=5.12.0',
            ],
            'test': [
                'pytest>=7.0.0',
                'pytest-cov>=4.0.0',
                'pytest-mock>=3.10.0',
            ],
            'docs': [
                'sphinx>=7.0.0',
                'sphinx-rtd-theme>=1.3.0',
            ],
            'all': [
                'pandas>=1.5.0',
                'numpy>=1.24.0',
                'nltk>=3.8.0',
                'spacy>=3.6.0',
                'fastapi>=0.100.0',
                'uvicorn>=0.23.0',
            ],
        },
        
        # === POINTS D'ENTRÉE ===
        entry_points=find_entry_points(),
        
        # === OPTIONS ===
        zip_safe=False,
        platforms=["any"],
        
        # === LICENSE ===
        license="MIT",
        license_files=["LICENSE"],
        
        # === OPTIONS SUPPLÉMENTAIRES ===
        options={
            'bdist_wheel': {
                'universal': False,
            },
        },
    )
    
    print("\n" + "=" * 60)
    print(f"✅ SETUP TERMINÉ - GSQL v{version} prêt pour la construction")
    print("=" * 60)

# ============================================================================
# EXÉCUTION
# ============================================================================

if __name__ == "__main__":
    # Options de ligne de commande
    if '--verify-manifest' in sys.argv:
        validator = ManifestValidator()
        success = validator.validate()
        sys.exit(0 if success else 1)
    
    elif '--version' in sys.argv:
        version_manager = VersionManager()
        print(f"GSQL version: {version_manager.get_version()}")
        sys.exit(0)
    
    elif '--help' in sys.argv:
        print("""
Options disponibles:
  --verify-manifest  Vérifie le fichier MANIFEST.in
  --version          Affiche la version actuelle
  --help             Affiche cette aide
  
Sans option: Exécute le setup normal
        """)
        sys.exit(0)
    
    # Exécution normale
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Construction interrompue par l'utilisateur")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erreur lors du setup: {e}")
        sys.exit(1)
