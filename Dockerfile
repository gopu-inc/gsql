# Utilisez une image plus spécifique et légère pour de meilleures performances
FROM python:3.9

# Définir la version comme argument de build - CORRECTION ICI
ARG VERSION=3.0.3

# Utiliser l'argument correctement - CORRECTION CRITIQUE ICI
LABEL version="3.0.3"
LABEL maintainer="ceoseshell"
LABEL description="GSQL - Graph Database Query Language"

# Définir les variables d'environnement tôt
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    NLTK_DATA=/usr/local/nltk_data

WORKDIR /app

# 1. Installer les outils système essentiels (slim nécessite plus d'outils)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    build-essential \
    python3-dev \
    curl \
    sqlite3 \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Mettre à jour pip et installer wheel
RUN pip install --upgrade pip setuptools wheel

# 3. Copier et installer les dépendances séparément (meilleure gestion du cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Installer GSQL AVANT de copier tout le code (optimisation du cache)
# CORRECTION : Installation avec la version spécifique


# 5. Télécharger les modèles NLTK (AVANT de copier le code pour optimiser)
RUN python -m nltk.downloader punkt stopwords wordnet averaged_perceptron_tagger -d ${NLTK_DATA}

# 6. Copier le reste du code source
COPY . .
RUN pip install -e .
# 7. Vérification que GSQL est installé avec la bonne version
RUN python -c "import gsql; print(f'GSQL version: {gsql.__version__}')"

# 8. Créer un utilisateur non-root (sécurité) - AMÉLIORATION
RUN addgroup --system --gid 1000 gsqluser && \
    adduser --system --uid 1000 --gid 1000 --disabled-password gsqluser

# 9. Changer les permissions AVANT de changer d'utilisateur
RUN chown -R gsqluser:gsqluser /app ${NLTK_DATA}

USER gsqluser

# 10. Point d'entrée
ENTRYPOINT ["gsql"]

# 11. Commande par défaut
CMD ["--help"]
