#!/usr/bin/env bash
set -e

echo "🚀 Stopping and removing any existing containers..."
docker rm -f mongo cassandra 2>/dev/null || true

echo "🚀 Starting MongoDB and Cassandra containers..."
docker-compose up -d

# --- Wait for Cassandra ---
echo "⏳ Waiting for Cassandra to be ready..."
until docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do
  sleep 3
done
echo "✅ Cassandra is ready!"

# --- Wait for MongoDB ---
echo "⏳ Waiting for MongoDB to be ready..."
until docker exec mongo mongosh --eval "db.adminCommand('ping')" >/dev/null 2>&1; do
  sleep 2
done
echo "✅ MongoDB is ready!"

# --- Python environment setup ---
VENV_DIR="venv"

if [ ! -d "$VENV_DIR" ]; then
    echo "🐍 Creating Python virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

echo "🐍 Activating virtual environment..."
# shellcheck disable=SC1091
. "$VENV_DIR/bin/activate"

echo "📦 Installing Python dependencies..."
pip install --upgrade pip
pip install pandas pymongo cassandra-driver

# --- Run import script ---
echo "📥 Importing CSV data into MongoDB and Cassandra..."
python import_data.py

echo "✅ Environment setup and data import complete!"
echo "ℹ️ To stop containers: docker-compose down"

# --- Run Mongo read queries ---
echo "📥 Running MongoDB queries..."
python3 queries_mongo.py

# --- Run Cassandra read/insert queries ---
echo "📥 Running Cassandra queries..."
python3 queries_cassandra.py
