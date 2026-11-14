#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${FDAP_MINIO_DATA_DIR:-${ROOT_DIR}/.minio-data}"
BUCKET="${FDAP_BUCKET:-fdap-warehouse}"
API_ADDR="${FDAP_MINIO_ADDR:-127.0.0.1:9000}"
CONSOLE_ADDR="${FDAP_MINIO_CONSOLE_ADDR:-127.0.0.1:9001}"
PID_FILE="${DATA_DIR}/minio.pid"
LOG_FILE="${DATA_DIR}/minio.log"

export MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
export MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
export NO_PROXY="127.0.0.1,localhost,${NO_PROXY:-}"
export no_proxy="127.0.0.1,localhost,${no_proxy:-}"

find_minio_bin() {
  if command -v minio >/dev/null 2>&1; then
    command -v minio
  elif [[ -x "${HOME}/.local/bin/minio" ]]; then
    echo "${HOME}/.local/bin/minio"
  else
    return 1
  fi
}

cmd="${1:-status}"

case "${cmd}" in
  start)
    mkdir -p "${DATA_DIR}/${BUCKET}"
    if curl -fsS "http://${API_ADDR}/minio/health/live" >/dev/null 2>&1; then
      echo "MinIO is already running at http://${API_ADDR} (bucket: ${BUCKET})"
      exit 0
    fi
    if MINIO_BIN="$(find_minio_bin)"; then
      echo "Starting local MinIO server at http://${API_ADDR} (console: http://${CONSOLE_ADDR})..."
      nohup "${MINIO_BIN}" server "${DATA_DIR}" \
        --address "${API_ADDR}" \
        --console-address "${CONSOLE_ADDR}" \
        >"${LOG_FILE}" 2>&1 &
      echo $! >"${PID_FILE}"
      for _ in {1..30}; do
        if curl -fsS "http://${API_ADDR}/minio/health/live" >/dev/null 2>&1; then
          echo "MinIO is ready! S3 endpoint: http://${API_ADDR} | Bucket: s3://${BUCKET}"
          exit 0
        fi
        sleep 0.2
      done
      echo "Failed to start MinIO; check ${LOG_FILE}" >&2
      exit 1
    elif command -v docker >/dev/null 2>&1; then
      echo "Starting MinIO via docker compose..."
      docker compose -f "${ROOT_DIR}/docker-compose.yml" up -d
    else
      echo "Neither 'minio' binary nor 'docker' found in PATH." >&2
      exit 1
    fi
    ;;
  stop)
    if [[ -f "${PID_FILE}" ]]; then
      PID="$(cat "${PID_FILE}")"
      if kill -0 "${PID}" 2>/dev/null; then
        echo "Stopping MinIO (PID ${PID})..."
        kill "${PID}"
      fi
      rm -f "${PID_FILE}"
    else
      pkill -f "minio server ${DATA_DIR}" 2>/dev/null || true
    fi
    ;;
  status)
    if curl -fsS "http://${API_ADDR}/minio/health/live" >/dev/null 2>&1; then
      echo "MinIO is healthy at http://${API_ADDR}"
    else
      echo "MinIO is not running at http://${API_ADDR}"
      exit 1
    fi
    ;;
  *)
    echo "Usage: $0 {start|stop|status}" >&2
    exit 1
    ;;
esac
