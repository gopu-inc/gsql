FROM python:3.9

ARG VERSION=0.1.1
LABEL version="3.0.4"

WORKDIR /app

# Installer les outils système
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    build-essential \
    python3-dev \
    curl \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Mettre à jour pip
RUN pip install --upgrade pip

# Copier et installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Télécharger les modèles NLTK
RUN python -m nltk.downloader punkt stopwords wordnet averaged_perceptron_tagger

# Copier le code source
COPY . .

# Installer GSQL
RUN pip install -e .

# Créer un utilisateur non-root
RUN useradd -m -u 1000 gsqluser && chown -R gsqluser:gsqluser /app
USER gsqluser

# Variables d'environnement
ENV NLTK_DATA=/home/gsqluser/nltk_data

# Point d'entrée
ENTRYPOINT ["gsql"]
CMD ["--help"]
