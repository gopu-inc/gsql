# Badges GSQL

Ce dossier contient tous les badges personnalisés pour le projet GSQL.

## Badges Disponibles

### Badges Principaux
| Badge | URL | Code Markdown |
|-------|-----|---------------|
| GSQL Database | `badges/gsql-database.svg` | `[![GSQL Database](URL)](https://github.com/gopu-inc/gsql)` |
| Version | `badges/version.svg` | `[![Version](URL)](https://github.com/gopu-inc/gsql)` |
| Python 3.8+ | `badges/python.svg` | `[![Python](URL)](https://python.org)` |

### Badges de Fonctionnalités
| Badge | Description | URL |
|-------|-------------|-----|
| NLP Enabled | Support NLP intégré | `badges/nlp-enabled.svg` |
| YAML Storage | Stockage YAML | `badges/yaml-storage.svg` |
| B-Tree Indexing | Indexation B-Tree | `badges/btree-indexing.svg` |

## Utilisation

### Dans README.md
```markdown
# Mon Projet

[![GSQL Database](badges/gsql-database.svg)](https://github.com/gopu-inc/gsql)
[![Version](badges/version.svg)](https://github.com/gopu-inc/gsql)
[![Python 3.8+](badges/python.svg)](https://python.org)
```

Dans la documentation

```html
<img src="badges/gsql-database.svg" alt="GSQL Database">
```

Génération Dynamique

Les badges sont générés dynamiquement via l'API Shields.io :

```
https://img.shields.io/badge/{label}-{message}-{color}?style={style}&logo={logo}&logoColor={logoColor}
```

API des Badges

Endpoints

· GET /badges/gsql-database.svg - Badge principal
· GET /badges/version.svg - Version actuelle
· GET /badges/nlp-enabled.svg - Support NLP

Paramètres

· style : flat, flat-square, plastic, for-the-badge
· color : Couleur hexadécimale (sans #)
· logo : Base64 SVG ou nom de logo
· logoColor : Couleur du logo

Exemples de Configuration

Configuration YAML

```yaml
badges:
  database:
    label: "GSQL"
    message: "Database"
    color: "4a6fa5"
    style: "for-the-badge"
    logo: "data:image/svg+xml;base64,..."
    
  version:
    label: "version"
    message: "1.0.0"
    color: "green"
    style: "flat-square"
```

Générateur Web

Utilisez le générateur sur notre site pour créer des badges personnalisés.

Mise à Jour

Pour mettre à jour les badges, éditez le fichier badges/config.yaml et relancez le script de génération :

```bash
python generate_badges.py
```

---

Note : Tous les badges sont générés dynamiquement et peuvent être personnalisés via les paramètres d'URL.

```

### **4. .github/workflows/deploy-badges.yml** - Workflow pour badges
```yaml
name: Deploy Badges to GitHub Pages

on:
  push:
    branches: [ main ]
    paths:
      - 'badges/**'
      - '.github/workflows/deploy-badges.yml'
  
  workflow_dispatch:
    inputs:
      message:
        description: 'Message de déploiement'
        required: false
        default: 'Mise à jour des badges'

permissions:
  contents: write
  pages: write
  id-token: write

jobs:
  generate-badges:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout repository
      uses: actions/checkout@v4
      with:
        fetch-depth: 0
    
    - name: Setup Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.10'
    
    - name: Install dependencies
      run: |
        pip install pyyaml requests pillow
    
    - name: Generate badges
      run: |
        python scripts/generate_badges.py
        mkdir -p page/badges
        cp badges/*.svg page/badges/
        cp badges/README.md page/badges/
        cp badges/config.yaml page/badges/
    
    - name: Deploy to GitHub Pages
      uses: peaceiris/actions-gh-pages@v3
      with:
        github_token: ${{ secrets.GITHUB_TOKEN }}
        publish_dir: ./page
        publish_branch: gh-pages
        user_name: 'github-actions[bot]'
        user_email: 'github-actions[bot]@users.noreply.github.com'
        commit_message: 'Deploy badges: ${{ github.event.head_commit.message || inputs.message }}'
```

5. scripts/generate_badges.py - Script de génération

```python
#!/usr/bin/env python3
"""
Script de génération des badges GSQL.
"""

import yaml
import requests
from pathlib import Path
import json

def generate_shields_badge(label, message, color, style="flat", logo=None, logoColor="white"):
    """Génère un badge Shields.io."""
    base_url = "https://img.shields.io/badge"
    
    # Encoder les paramètres
    label_encoded = label.replace(" ", "_").replace("-", "--")
    message_encoded = message.replace(" ", "_").replace("-", "--")
    
    # Construire l'URL
    url = f"{base_url}/{label_encoded}-{message_encoded}-{color}?style={style}"
    
    if logo:
        url += f"&logo={logo}"
        url += f"&logoColor={logoColor}"
    
    return url

def download_badge(url, output_path):
    """Télécharge un badge."""
    response = requests.get(url)
    response.raise_for_status()
    
    with open(output_path, 'wb') as f:
        f.write(response.content)
    
    print(f"✓ Badge généré: {output_path}")

def main():
    """Fonction principale."""
    # Charger la configuration
    config_path = Path("badges/config.yaml")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Créer le dossier badges s'il n'existe pas
    badges_dir = Path("badges")
    badges_dir.mkdir(exist_ok=True)
    
    # Générer les badges
    for badge_name, badge_config in config["badges"].items():
        print(f"Génération du badge: {badge_name}")
        
        # Générer l'URL du badge
        badge_url = generate_shields_badge(
            label=badge_config.get("label", badge_name),
            message=badge_config.get("message", ""),
            color=badge_config.get("color", "blue"),
            style=badge_config.get("style", "flat-square"),
            logo=badge_config.get("logo"),
            logoColor=badge_config.get("logoColor", "white")
        )
        
        # Télécharger le badge
        output_file = badges_dir / f"{badge_name}.svg"
        download_badge(badge_url, output_file)
        
        # Créer un fichier JSON avec les métadonnées
        metadata = {
            "name": badge_name,
            "url": badge_url,
            "markdown": f"[![{badge_config.get('label')}]({badge_url})](https://github.com/gopu-inc/gsql)",
            "html": f'<img src="{badge_url}" alt="{badge_config.get("label")}">',
            "config": badge_config
        }
        
        metadata_file = badges_dir / f"{badge_name}.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    # Générer le fichier d'index
    index_content = "# Badges GSQL\n\n"
    index_content += "## Tous les badges disponibles\n\n"
    
    for badge_file in badges_dir.glob("*.json"):
        with open(badge_file, 'r') as f:
            metadata = json.load(f)
        
        index_content += f"### {metadata['name']}\n"
        index_content += f"![{metadata['config'].get('label')}]({metadata['url']})\n\n"
        index_content += f"**Markdown:**\n```markdown\n{metadata['markdown']}\n```\n\n"
        index_content += f"**HTML:**\n```html\n{metadata['html']}\n```\n\n"
        index_content += "---\n\n"
    
    index_file = badges_dir / "INDEX.md"
    with open(index_file, 'w') as f:
        f.write(index_content)
    
    print("\n✅ Tous les badges ont été générés avec succès!")

if __name__ == "__main__":
    main()
```

6. badges/config.yaml - Configuration des badges

```yaml
# Configuration des badges GSQL
badges:
  gsql-database:
    label: "GSQL"
    message: "Database"
    color: "4a6fa5"
    style: "for-the-badge"
    logo: "data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTAyNCIgaGVpZ2h0PSIxMDI0IiB2aWV3Qm94PSIwIDAgMTAyNCAxMDI0IiBmaWxsPSJub25lIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPjxyZWN0IHdpZHRoPSIxMDI0IiBoZWlnaHQ9IjEwMjQiIGZpbGw9IiM0YTZmYTUiLz48dGV4dCB4PSI1MTIiIHk9IjU0MCIgZm9udC1mYW1pbHk9IkFyaWFsIiBmb250LXNpemU9IjQwMCIgdGV4dC1hbmNob3I9Im1pZGRsZSIgZmlsbD0id2hpdGUiPkc8L3RleHQ+PC9zdmc+"
    logoColor: "white"
  
  version:
    label: "version"
    message: "1.0.0"
    color: "green"
    style: "flat-square"
  
  python:
    label: "python"
    message: "3.8+"
    color: "3776AB"
    style: "flat-square"
    logo: "python"
    logoColor: "white"
  
  nlp-enabled:
    label: "GSQL"
    message: "NLP Enabled"
    color: "4FC3A1"
    style: "for-the-badge"
    logo: "ai"
    logoColor: "white"
  
  yaml-storage:
    label: "Storage"
    message: "YAML"
    color: "blueviolet"
    style: "for-the-badge"
    logo: "yaml"
    logoColor: "white"
  
  btree-indexing:
    label: "B-Tree"
    message: "Indexing"
    color: "FF6B35"
    style: "for-the-badge"
    logo: "tree"
    logoColor: "white"
  
  license:
    label: "license"
    message: "MIT"
    color: "green"
    style: "flat-square"
  
  stars:
    label: "GitHub"
    message: "stars"
    color: "black"
    style: "social"
    logo: "github"
  
  downloads:
    label: "PyPI"
    message: "downloads"
    color: "blue"
    style: "flat-square"
    logo: "pypi"

# Couleurs disponibles
colors:
  primary: "4a6fa5"
  secondary: "166088"
  accent: "4fc3a1"
  success: "28a745"
  warning: "ffc107"
  danger: "dc3545"
  info: "17a2b8"

# Styles disponibles
styles:
  - "flat"
  - "flat-square"
  - "plastic"
  - "for-the-badge"
  - "social"
```

📂 Structure finale du dossier page/

```
page/
├── index.html                          # Page d'accueil principale
├── database.html                       # Documentation Database
├── parser.html                         # Documentation Parser
├── storage.html                        # Documentation Storage
├── nlp.html                           # Documentation NLP
├── btree.html                          # Documentation B-Tree
├── functions.html                      # Documentation Functions
├── api.html                           # Documentation API complète
│
├── badges/                            # Dossier des badges
│   ├── gsql-database.svg
│   ├── version.svg
│   ├── python.svg
│   ├── nlp-enabled.svg
│   ├── yaml-storage.svg
│   ├── btree-indexing.svg
│   ├── config.yaml
│   ├── README.md
│   └── INDEX.md
│
├── assets/                            # Ressources
│   ├── logo.svg
│   ├── logo.png
│   └── favicon.ico
│
├── css/                               # Styles CSS
│   ├── main.css
│   └── badges.css
│
└── js/                                # JavaScript
    ├── main.js
    ├── badge-generator.js
    └── copy-to-clipboard.js
```

🚀 Instructions de déploiement

1. Créer la structure de dossiers :

```bash
mkdir -p page/{badges,assets,css,js}
```

1. Placer les fichiers :
