#!/usr/bin/env bash
set -euo pipefail

# Configurable via env vars or flags
API_BASE=${API_BASE:-http://localhost:8000}
GENRE=${GENRE:-loadtest}
LOCATION=${LOCATION:-nyc}
AUTHOR_PREFIX=${AUTHOR_PREFIX:-apitest}
MODE=${MODE:-crud}             # health|crud|load
COUNT=${COUNT:-20}             # for load mode
CONCURRENCY=${CONCURRENCY:-5}  # for load mode
WAIT_SECS=${WAIT_SECS:-3}      # initial wait after enqueue before reading
WAIT_MAX_SECS=${WAIT_MAX_SECS:-10}  # max poll seconds to wait for visibility

usage() {
  cat <<USAGE
Usage: API_BASE=http://localhost:8000 MODE=[health|crud|load] \
       GENRE=loadtest LOCATION=nyc COUNT=50 CONCURRENCY=10 \
       WAIT_SECS=3 WAIT_MAX_SECS=10 \
       bash scripts/test_api.sh

Defaults:
  API_BASE=$API_BASE
  MODE=$MODE
  GENRE=$GENRE LOCATION=$LOCATION
  COUNT=$COUNT CONCURRENCY=$CONCURRENCY
  WAIT_SECS=$WAIT_SECS WAIT_MAX_SECS=$WAIT_MAX_SECS
USAGE
}

jq_get() {
  local key=$1
  if command -v jq >/dev/null 2>&1; then
    jq -r ".${key}" 2>/dev/null || true
  else
    python3 - "$key" 2>/dev/null <<'PY'
import sys, json
key = sys.argv[1]
try:
  obj = json.load(sys.stdin)
  v = obj.get(key, "")
  print(v if v is not None else "")
except Exception:
  pass
PY
  fi
}

extract_first_id_from_list() {
  if command -v jq >/dev/null 2>&1; then
    jq -r '.[0].id' 2>/dev/null || true
  else
    python3 - 2>/dev/null <<'PY'
import sys, json
try:
  a = json.load(sys.stdin)
  print(a[0]['id'] if a else "")
except Exception:
  pass
PY
  fi
}

wait_for_visible() {
  local author="$1"; local genre="$2"; local location="$3"; local limit="$4"
  local waited=0
  while (( waited < WAIT_MAX_SECS )); do
    resp=$(curl -s "$API_BASE/blogs?author=$author&genre=$genre&location=$location&limit=$limit")
    id=$(echo "$resp" | extract_first_id_from_list)
    if [[ -n "$id" && "$id" != "null" ]]; then
      echo "$resp"
      return 0
    fi
    sleep 1
    waited=$(( waited + 1 ))
  done
  echo ""  # empty result after timeout
  return 1
}

health() {
  echo "== Health =="
  curl -sf "$API_BASE/healthz" | cat
  echo
}

crud() {
  local author="${AUTHOR_PREFIX}_$RANDOM"
  echo "== Create =="
  create_resp=$(curl -s -X POST "$API_BASE/blogs" \
    -H 'Content-Type: application/json' \
    -d "{\"author\":\"$author\",\"content\":\"hello via api test\",\"genre\":\"$GENRE\",\"location\":\"$LOCATION\"}")
  echo "$create_resp"

  echo "== Wait $WAIT_SECS s for worker flush =="
  sleep "$WAIT_SECS"

  echo "== List filtered (poll up to $WAIT_MAX_SECS s) =="
  list_resp=$(wait_for_visible "$author" "$GENRE" "$LOCATION" 50 || true)
  echo "${list_resp:-[]}" | cat
  echo
  blog_id=$(echo "${list_resp:-}" | extract_first_id_from_list)
  if [[ -z "$blog_id" || "$blog_id" == "null" ]]; then
    echo "No blog found for author=$author after waiting; try increasing WAIT_MAX_SECS or check worker logs" >&2
    exit 1
  fi
  echo "Picked BLOG_ID=$blog_id"

  echo "== Read by id =="
  curl -s "$API_BASE/blogs/$blog_id" | cat
  echo

  echo "== Update =="
  curl -s -X PUT "$API_BASE/blogs/$blog_id" \
    -H 'Content-Type: application/json' \
    -d '{"content":"updated content via test"}' | cat
  echo

  echo "== Verify =="
  curl -s "$API_BASE/blogs/$blog_id" | cat
  echo

  echo "== Delete =="
  curl -s -X DELETE "$API_BASE/blogs/$blog_id" | cat
  echo

  echo "== Verify 404 =="
  curl -i -s "$API_BASE/blogs/$blog_id" | head -n1
}

load() {
  local author="${AUTHOR_PREFIX}_$RANDOM"
  echo "== Load enqueue: COUNT=$COUNT CONCURRENCY=$CONCURRENCY author=$author =="
  t_start=$(date +%s)
  pids=()
  for ((i=1;i<=COUNT;i++)); do
    payload=$(printf '{"author":"%s","content":"msg %d","genre":"%s","location":"%s"}' "$author" "$i" "$GENRE" "$LOCATION")
    curl -s -X POST "$API_BASE/blogs" -H 'Content-Type: application/json' -d "$payload" >/dev/null &
    pids+=("$!")
    if (( i % CONCURRENCY == 0 )); then
      wait
      pids=()
    fi
  done
  wait || true
  t_end=$(date +%s)
  duration=$((t_end - t_start))
  echo "Enqueued $COUNT in ${duration}s (~$(( COUNT / (duration>0?duration:1) )) req/s)"

  echo "== Wait $WAIT_SECS s for worker flush =="
  sleep "$WAIT_SECS"

  echo "== List filtered (poll up to $WAIT_MAX_SECS s) =="
  resp=$(wait_for_visible "$author" "$GENRE" "$LOCATION" "$COUNT" || true)
  cnt=$(echo "${resp:-[]}" | {
    if command -v jq >/dev/null 2>&1; then jq 'length'; else python3 - <<'PY'
import sys,json
try:
  print(len(json.load(sys.stdin)))
except Exception:
  print(0)
PY
    fi
  })
  echo "Found $cnt rows for author=$author"
}

main() {
  case "${1:-$MODE}" in
    -h|--help|help) usage ;;
    health) health ;;
    crud) health; crud ;;
    load) health; load ;;
    *) echo "Unknown mode: ${1:-$MODE}"; usage; exit 2 ;;
  esac
}

main "$@" 