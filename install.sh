#!/bin/bash

set -e

echo "🚀 Installation complète Hadoop + WordCount — MODE AUTOMATIQUE"

# ================================
# 0) Vérifs de base
# ================================
command -v docker >/dev/null 2>&1 || { echo "❌ Docker n'est pas installé."; exit 1; }
command -v docker compose >/dev/null 2>&1 || { echo "❌ Docker Compose n'est pas installé."; exit 1; }

echo "✔️ Docker OK"

# ================================
# 1) Lancer Hadoop Cluster
# ================================
echo "🐳 Lancement du cluster Hadoop..."
docker compose up -d

echo "⏳ Attente 5sec que les conteneurs Hadoop soient opérationnels..."
sleep 5

# ================================
# 2) Lancer Dev Container
# ================================
echo "🐧 Lancement du conteneur Dev..."
docker compose -f docker-compose.dev.yml up -d

sleep 3

# ================================
# 3) Installer Java dans Dev
# ================================
echo "☕ Installation de Java dans le conteneur dev..."

docker exec -it hadoop-dev bash -c "
  apt update -y &&
  apt install -y openjdk-11-jdk &&
  javac -version
"

echo "✔️ Java installé"


