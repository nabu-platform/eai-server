#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  invoke.sh <serviceId> [-e <endpoint>] [-p <jsonFile>] [options]

Required:
  serviceId  Service ID (e.g. my.service)

Optional:
  -e  Endpoint base URL (default: http://localhost:5555)
  -p  JSON file path (payload). If omitted, uses {}
  -u  Run-As (username)
  -r  Run-As-Realm
  -c  Service-Context
  -f  Feature (can be repeated)
  -H  Extra header (can be repeated, e.g. "X-Foo: bar")

Example:
  invoke.sh my.service \
    -u alice -r internal -c ops -f beta -f audit

  invoke.sh my.service -p payload.json
EOF
}

endpoint="http://localhost:5555"
service_id=""
payload=""
run_as=""
run_as_realm=""
service_context=""
declare -a features
declare -a extra_headers

while getopts ":e:p:u:r:c:f:H:h" opt; do
  case "$opt" in
    e) endpoint="$OPTARG" ;;
    p) payload="$OPTARG" ;;
    u) run_as="$OPTARG" ;;
    r) run_as_realm="$OPTARG" ;;
    c) service_context="$OPTARG" ;;
    f) features+=("$OPTARG") ;;
    H) extra_headers+=("$OPTARG") ;;
    h) usage; exit 0 ;;
    *) usage; exit 1 ;;
  esac
done

if [[ $# -gt $((OPTIND - 1)) ]]; then
  service_id="${!OPTIND}"
fi

if [[ -z "$service_id" ]]; then
  usage
  exit 1
fi

endpoint="${endpoint%/}"
url="${endpoint}/invoke/${service_id}"

headers=( "--header=Content-Type: application/json" )
if [[ -n "$run_as" ]]; then
  headers+=( "--header=Run-As: $run_as" )
fi
if [[ -n "$run_as_realm" ]]; then
  headers+=( "--header=Run-As-Realm: $run_as_realm" )
fi
if [[ -n "$service_context" ]]; then
  headers+=( "--header=Service-Context: $service_context" )
fi
if [[ ${#features[@]} -gt 0 ]]; then
  for feature in "${features[@]}"; do
    headers+=( "--header=Feature: $feature" )
  done
fi
if [[ ${#extra_headers[@]} -gt 0 ]]; then
  for h in "${extra_headers[@]}"; do
    headers+=( "--header=$h" )
  done
fi

if [[ -n "$payload" ]]; then
  wget --method=POST \
    "${headers[@]}" \
    --body-file="$payload" \
    "$url"
else
  wget --method=POST \
    "${headers[@]}" \
    --body-data="{}" \
    "$url"
fi
