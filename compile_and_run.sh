#!/bin/bash

set -e

# === CONFIG ===
# Default values for configuration.
# These can be overridden with command-line flags.
CONTAINER="namenode"
DEFAULT_INPUT_FILE="./src/wordcount/file.txt"
JAR_NAME="wc.jar"
MAIN_CLASS="src.wordcountenseignant.WCDriver"

# --- HDFS/Internal Paths ---
REMOTE_SRC_DIR="/root/src"
HDFS_INPUT="/data"
HDFS_OUTPUT="/output"

# --- Functions ---
usage() {
    echo "Usage: $0 [options] [input_file]"
    echo ""
    echo "Runs the Hadoop WordCount job in a Docker container."
    echo ""
    echo "Positional Arguments:"
    echo "  input_file         Path to the local input file. (Default: $DEFAULT_INPUT_FILE)"
    echo ""
    echo "Options:"
    echo "  -container=<name>  Docker container name. (Default: $CONTAINER)"
    echo "  -jarName=<name>    Name of the .jar file to create. (Default: $JAR_NAME)"
    echo "  -mainClass=<class> Java main class to run. (Default: $MAIN_CLASS)"
    echo "  -h, --help         Show this help message."
    exit 1
}

# --- Argument Parsing ---
# Store positional arguments here
INPUT_FILE=""

# Loop while arguments exist to parse flags
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -container=*)
        CONTAINER="${1#*=}" # Set variable to value after '='
        shift # Move to next argument
        ;;
        -jarName=*)
        JAR_NAME="${1#*=}"
        shift
        ;;
        -mainClass=*)
        MAIN_CLASS="${1#*=}"
        shift
        ;;
        -h|--help)
        usage
        ;;
        -*)
        # Unknown option
        echo "Error: Unknown option: $1"
        usage
        ;;
        *)
        # Positional argument
        if [ -z "$INPUT_FILE" ]; then
            INPUT_FILE="$1"
        else
            echo "Error: Too many positional arguments. Only one input file is allowed."
            usage
        fi
        shift # Move to next argument
        ;;
    esac
done

# After loop, set default input file if not provided
if [ -z "$INPUT_FILE" ]; then
    INPUT_FILE="$DEFAULT_INPUT_FILE"
fi

# --- Validation ---
if [ ! -f "$INPUT_FILE" ] && [ ! -d "$INPUT_FILE" ]; then
    echo "Error: '$INPUT_FILE' not found (neither file nor directory)."
    usage
fi

# --- Derived variables ---
INPUT_FILE_NAME=$(basename "$INPUT_FILE")


# --- Main Script ---
echo "🚀 Déploiement WordCount Hadoop..."
echo "   Container:  $CONTAINER"
echo "   Input:      $INPUT_FILE"
echo "   Jar Name:   $JAR_NAME"
echo "   Main Class: $MAIN_CLASS"


# 1) Copie du code Java dans le container
echo "📁 Copie du dossier src vers le conteneur..."
docker cp ./src "$CONTAINER:/root/"

# 2) Copie du fichier d'entrée
echo "📄 Copie du fichier d'entrée dans le conteneur..."
docker cp "$INPUT_FILE" "$CONTAINER:/root/"

# 3) Compilation Java
echo "🛠️ Compilation des fichiers Java..."
docker exec -it $CONTAINER bash -c "
    rm -rf /root/build && mkdir -p /root/build &&
    find $REMOTE_SRC_DIR -name '*.java' > /root/sources.txt &&
    javac -encoding UTF-8 -cp \"\$(hadoop classpath)\" -d /root/build @/root/sources.txt
"

# 4) Création du JAR
echo "📦 Création du JAR..."
docker exec -it $CONTAINER bash -c "
    jar -cvf $JAR_NAME -C /root/build .
"

# 5) Préparation HDFS
echo "🗑️ Nettoyage ancien /output si existe..."
docker exec -it $CONTAINER hdfs dfs -rm -r -f $HDFS_OUTPUT || true

echo "📁 Upload du fichier/dossier dans HDFS..."
docker exec -it $CONTAINER hdfs dfs -mkdir -p $HDFS_INPUT

# Si INPUT_FILE est un dossier local (ex: ./data)
if [ -d "$INPUT_FILE" ]; then
    # On envoie tous les fichiers du dossier dans HDFS:/data
    docker exec -it $CONTAINER bash -c "hdfs dfs -put -f /root/$INPUT_FILE_NAME/* $HDFS_INPUT"
else
    # Cas ancien : un seul fichier
    docker exec -it $CONTAINER bash -c "hdfs dfs -put -f /root/$INPUT_FILE_NAME $HDFS_INPUT"
fi


# 6) Exécution du job WordCount
echo "⚙️ Exécution du job Hadoop..."
docker exec -it $CONTAINER bash -c "
    hadoop jar $JAR_NAME $MAIN_CLASS $HDFS_INPUT $HDFS_OUTPUT || echo '⚠️ Hadoop a renvoyé une erreur (JobHistory), mais les jobs ont peut-être réussi.'
"

# 7) Lecture du résultat
echo "📊 Résultat final :"
docker exec -it $CONTAINER hdfs dfs -cat $HDFS_OUTPUT/part-r-00000

echo "✅ FINI !"