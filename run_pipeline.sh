#!/usr/bin/env bash
# One-command pipeline: download -> ingest (MinIO raw) -> clean -> analytics
# -> serving (PostgreSQL) -> row-count reconciliation.
#
# Usage:
#   ./run_pipeline.sh                  # run every step
#   ./run_pipeline.sh --from clean     # resume from a step
#   ./run_pipeline.sh --only ingest    # run a single step
#   ./run_pipeline.sh --list           # show step names
#
# Stops at the first failing step. Each run is logged to logs/pipeline_<ts>.log.

set -Eeuo pipefail

# Git Bash on Windows rewrites /opt/... arguments into Windows paths.
export MSYS_NO_PATHCONV=1

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

STEPS=(infra download ingest clean review_analytics join_analytics reconcile)

SPARK_MASTER_URL_IN_CLUSTER="spark://spark-master:7077"
JOBS_DIR_IN_CLUSTER="/opt/spark/jobs"

LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"
RUN_ID="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/pipeline_${RUN_ID}.log"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source <(sed 's/\r$//' .env)
  set +a
fi

if command -v python >/dev/null 2>&1; then
  PYTHON="${PYTHON:-python}"
else
  PYTHON="${PYTHON:-python3}"
fi

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" | tee -a "$LOG_FILE"
}

CURRENT_STEP="setup"

# Stopping the local docker client (Ctrl+C, crash) does not stop the driver
# inside spark-master, so kill it there too. Harmless when nothing is running.
stop_spark_driver() {
  docker compose exec -T spark-master pkill -f org.apache.spark.deploy.SparkSubmit >/dev/null 2>&1 || true
}

on_error() {
  local code=$?
  trap - ERR
  stop_spark_driver
  log "FAILED at step '${CURRENT_STEP}' (exit code ${code}). See ${LOG_FILE}"
  exit "$code"
}
trap on_error ERR

on_interrupt() {
  trap - ERR
  stop_spark_driver
  log "Interrupted during step '${CURRENT_STEP}'"
  exit 130
}
trap on_interrupt INT TERM

# Run a command, streaming its output to the console and the log file.
run() {
  "$@" 2>&1 | tee -a "$LOG_FILE"
}

spark_submit() {
  local job="$1"
  shift
  run docker compose exec -T spark-master /opt/spark/bin/spark-submit \
    --master "$SPARK_MASTER_URL_IN_CLUSTER" \
    --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY:-1536m}" \
    --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY:-1g}" \
    "$JOBS_DIR_IN_CLUSTER/$job" "$@"
}

wait_healthy() {
  local service="$1" tries=30
  until [[ "$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q "$service")" 2>/dev/null)" == "healthy" ]]; do
    ((tries--)) || { log "$service is not healthy"; return 1; }
    sleep 5
  done
  log "$service is healthy"
}

wait_spark_workers() {
  local expected="${1:-2}" tries=30 alive
  while true; do
    alive="$(curl -fsS http://localhost:8080/json/ 2>/dev/null \
      | grep -o '"aliveworkers" *: *[0-9]*' | grep -o '[0-9]*$' || true)"
    [[ "${alive:-0}" -ge "$expected" ]] && break
    ((tries--)) || { log "Only ${alive:-0}/${expected} Spark workers alive"; return 1; }
    sleep 5
  done
  log "Spark workers alive: $alive"
}

# Idempotent: CREATE TABLE/INDEX IF NOT EXISTS, nên chạy lại mỗi lần đều an toàn.
# Cần thiết khi volume postgres đã tồn tại từ trước (init.sql không tự chạy lại).
apply_serving_ddl() {
  log "Applying postgres/init.sql (Serving Layer DDL)"
  docker compose exec -T postgres \
    sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
    < postgres/init.sql 2>&1 | tee -a "$LOG_FILE"
}

# ---------------------------------------------------------------- steps

step_infra() {
  # Build only when a locally built image is missing (e.g. first run).
  if docker image inspect bdt301/spark:3.5.6 >/dev/null 2>&1 \
    && docker image inspect "quay.io/minio/minio:RELEASE.2025-09-06T17-38-46Z" >/dev/null 2>&1; then
    run docker compose up -d --no-build
  else
    run docker compose up -d --build
  fi
  wait_healthy minio
  wait_healthy postgres
  # Bucket processed phải có trước Job 1 (S3A không tự tạo bucket).
  run "$PYTHON" ingestion/ingest.py --buckets-only
  apply_serving_ddl
  wait_spark_workers 2
}

step_download() {
  if [[ -f data/raw/books_data.csv && -f data/raw/Books_rating.csv ]]; then
    log "Raw CSVs already in data/raw, skipping download (delete them to re-download)"
    return 0
  fi
  run "$PYTHON" ingestion/download_kaggle_dataset.py
}

step_ingest() {
  run "$PYTHON" ingestion/ingest.py
}

step_clean() {
  spark_submit clean_data_job.py
}

step_review_analytics() {
  spark_submit review_analytics_job.py
}

step_join_analytics() {
  # Job 2a (TV3): join reviews with books and write serving tables.
  local job="${JOIN_JOB:-join_analytics_job.py}"
  if [[ ! -f "spark/jobs/$job" ]]; then
    log "ERROR: spark/jobs/$job not found (Job 2a is required)"
    return 1
  fi
  spark_submit "$job"
}

step_reconcile() {
  spark_submit reconcile_counts.py
}

# ---------------------------------------------------------------- main

usage() {
  sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
}

index_of() {
  local i
  for i in "${!STEPS[@]}"; do
    [[ "${STEPS[$i]}" == "$1" ]] && { echo "$i"; return 0; }
  done
  echo "Unknown step: $1 (use --list)" >&2
  return 1
}

start=0
end=$((${#STEPS[@]} - 1))
case "${1:-}" in
  "") ;;
  --from) start="$(index_of "${2:?step name required}")" ;;
  --only) start="$(index_of "${2:?step name required}")"; end="$start" ;;
  --list) printf '%s\n' "${STEPS[@]}"; exit 0 ;;
  -h|--help) usage; exit 0 ;;
  *) usage; exit 2 ;;
esac

log "Pipeline run ${RUN_ID} started (steps: ${STEPS[*]:start:end-start+1})"
pipeline_started=$SECONDS

for ((i = start; i <= end; i++)); do
  CURRENT_STEP="${STEPS[$i]}"
  log "==> [$((i + 1))/${#STEPS[@]}] ${CURRENT_STEP}"
  step_started=$SECONDS
  "step_${CURRENT_STEP}"
  log "<== ${CURRENT_STEP} OK ($((SECONDS - step_started))s)"
done

log "Pipeline run ${RUN_ID} finished in $((SECONDS - pipeline_started))s"
