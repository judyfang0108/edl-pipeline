#!/usr/bin/env bash
# setup.sh — Bring up the full CDC stack and run the Flink pipeline.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

log() { echo "[$(date +%T)] $*"; }
wait_for() {
  local label=$1 url=$2
  log "Waiting for $label..."
  until curl -sf "$url" > /dev/null 2>&1; do sleep 2; done
  log "$label is ready."
}

# ── 1. Infrastructure ────────────────────────────────────────────────────────
log "Starting Docker services..."
docker compose up -d

wait_for "Debezium"   "http://localhost:8083/"
wait_for "ClickHouse" "http://localhost:8123/ping"
wait_for "Flink UI"   "http://localhost:8081/"

# ── 2. MySQL: create tables & seed data ─────────────────────────────────────
log "Setting up MySQL source tables..."
until docker compose exec -T mysql mysqladmin ping -h localhost -prootpass --silent 2>/dev/null; do
  sleep 2
done
docker compose exec -T mysql mysql -u root -prootpass < sql/mysql_source.sql
log "MySQL ready."

# ── 3. ClickHouse: create destination tables ─────────────────────────────────
log "Setting up ClickHouse destinations..."
docker compose exec -T clickhouse clickhouse-client --multiquery < sql/clickhouse_dest.sql
log "ClickHouse ready."

# ── 4. Debezium: register connector ──────────────────────────────────────────
log "Registering Debezium connector..."
curl -sf -X DELETE "http://localhost:8083/connectors/mysql-cdc-source" 2>/dev/null || true
curl -sf -X POST "http://localhost:8083/connectors" \
  -H "Content-Type: application/json" \
  -d @config/debezium_connector.json > /dev/null
sleep 5
STATUS=$(curl -sf "http://localhost:8083/connectors/mysql-cdc-source/status" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])")
log "Debezium connector state: $STATUS"
if [ "$STATUS" != "RUNNING" ]; then
  echo "ERROR: Debezium connector is not running!" >&2
  exit 1
fi

# Wait for topics to appear
log "Waiting for Kafka topics..."
until docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q 'cdc.source_db.orders'; do
  sleep 3
done
log "Kafka topics ready."

# ── 5. Install PyFlink in Flink containers (only on first run) ────────────────
install_pyflink() {
  local container=$1
  if docker exec "$container" python3 -c "import pyflink" 2>/dev/null; then
    log "PyFlink already installed in $container."
    return
  fi
  log "Installing Python + PyFlink in $container (one-time, ~3 min)..."
  docker exec -u root "$container" bash -c "
    apt-get update -qq &&
    apt-get install -y python3 python3-pip openjdk-11-jdk-headless -qq &&
    ln -sf /usr/bin/python3 /usr/bin/python &&
    cp -r /usr/lib/jvm/java-11-openjdk-\$(dpkg --print-architecture)/include /opt/java/openjdk/ &&
    pip3 install --quiet 'apache-flink==1.18.0'
  "
  # Download Kafka connector JAR
  docker exec "$container" bash -c "
    ls /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar 2>/dev/null ||
    curl -sL -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
  "
  log "PyFlink installed in $container."
}

JM=$(docker compose ps -q flink-jobmanager)
TM=$(docker compose ps -q flink-taskmanager)
install_pyflink "$JM"
install_pyflink "$TM"

# ── 6. Copy pipeline and launch ──────────────────────────────────────────────
log "Copying pipeline to Flink jobmanager..."
docker cp src/pipeline.py "$JM":/opt/flink/jobs/pipeline.py
docker cp config/pipelines.json "$JM":/opt/flink/jobs/pipelines.json

log "Submitting Flink job..."
docker exec -d "$JM" bash -c "python3 /opt/flink/jobs/pipeline.py > /tmp/pipeline.log 2>&1"

sleep 10
log "Checking Flink job status..."
curl -sf http://localhost:8081/jobs 2>/dev/null || echo "(check /tmp/pipeline.log inside jobmanager)"

log ""
log "Done! Try:"
log "  docker compose exec -T mysql mysql -u root -prootpass source_db -e \\"
log "    \"UPDATE orders SET amount=999.99, status='shipped' WHERE id=1;\""
log ""
log "  docker compose exec -T clickhouse clickhouse-client --query \\"
log "    \"SELECT * FROM dest_db.order_summary_live FORMAT Pretty\""
