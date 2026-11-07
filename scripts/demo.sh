#!/usr/bin/env bash
# Runs the order-processing example against a running GoFlow API.
# Usage: GOFLOW_API_KEY=gf_... ./scripts/demo.sh
set -euo pipefail

API="${GOFLOW_API_URL:-http://localhost:8080}"
KEY="${GOFLOW_API_KEY:?set GOFLOW_API_KEY (make apikey)}"
DIR="$(cd "$(dirname "$0")/.." && pwd)"
auth=(-H "Authorization: Bearer ${KEY}" -H "Content-Type: application/json")

json() { python3 -c "import sys, json; print(json.load(sys.stdin)$1)"; }

echo "==> creating workflow"
# Workflow names are unique per tenant; suffix the example so the demo can be re-run.
BODY=$(python3 -c 'import json, sys, time
d = json.load(open(sys.argv[1])); d["name"] += "-" + time.strftime("%Y%m%d%H%M%S")
print(json.dumps(d))' "$DIR/examples/order-processing.json")
WF=$(curl -fsS "${auth[@]}" -X POST "$API/api/v1/workflows" -d "$BODY" | json '["id"]')
curl -fsS "${auth[@]}" -X POST "$API/api/v1/workflows/$WF/publish" >/dev/null
echo "    workflow $WF published"

start() {
  curl -fsS "${auth[@]}" -X POST "$API/api/v1/executions" -H "Idempotency-Key: $1" \
    -d "{\"workflow_id\":\"$WF\",\"input\":$2}" | json '["id"]'
}

OK=$(start "demo-ok-$RANDOM" '{"order_id":"o-1","customer_id":"c-1","amount":120,"items":[{"sku":"SKU-1","quantity":2}],"simulate":{"invoice":"transient:1"}}')
FAIL=$(start "demo-fail-$RANDOM" '{"order_id":"o-2","customer_id":"c-1","amount":80,"items":[{"sku":"SKU-2","quantity":1}],"simulate":{"invoice":"permanent"}}')
echo "==> started $OK (succeeds after one retry) and $FAIL (compensated)"

for id in "$OK" "$FAIL"; do
  for _ in $(seq 1 60); do
    status=$(curl -fsS "${auth[@]}" "$API/api/v1/executions/$id" | json '["status"]')
    case "$status" in COMPLETED|FAILED|CANCELLED|COMPENSATED) break ;; esac
    sleep 0.5
  done
  echo "==> $id: $status"
  curl -fsS "${auth[@]}" "$API/api/v1/executions/$id/history?limit=200" |
    python3 -c 'import sys, json
for e in json.load(sys.stdin)["items"]:
    print("    %-26s %-9s %s -> %s %s" % (e["type"], e.get("step_key", ""), e.get("from", ""), e.get("to", ""), e.get("message", "")[:70]))'
done
