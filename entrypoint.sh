#!/usr/bin/env bash
set -euo pipefail

max_attempts=10
attempt=1
until alembic upgrade head; do
  echo "Alembic failed (attempt $attempt/$max_attempts), retrying in 3s..."
  if [ "$attempt" -ge "$max_attempts" ]; then
    echo "Alembic failed after $max_attempts attempts, exiting."
    exit 1
  fi
  attempt=$((attempt + 1))
  sleep 3
done

exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
