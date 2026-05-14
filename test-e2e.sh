#!/bin/bash
# E2E test script for hermes-agent-cluster cluster
set -e

MAIN="http://localhost:8787"
WORKER="http://localhost:8788"

echo "========================================="
echo "  hermes-agent-cluster E2E Test"
echo "========================================="

# --- Test 1: Health check ---
echo ""
echo "[1/8] Health check..."
curl -sf $MAIN/api/v1/nodes > /dev/null && echo "  ✅ Main node healthy" || echo "  ❌ Main node unreachable"
curl -sf $WORKER/api/v1/nodes > /dev/null && echo "  ✅ Worker node healthy" || echo "  ❌ Worker node unreachable"

# --- Test 2: List nodes (main should see itself) ---
echo ""
echo "[2/8] List nodes on main..."
NODES=$(curl -sf $MAIN/api/v1/nodes)
echo "  $NODES" | python3 -m json.tool 2>/dev/null || echo "  $NODES"

# --- Test 3: Worker joins cluster ---
echo ""
echo "[3/8] Worker joins cluster via main..."
JOIN_RESP=$(curl -sf -X POST $MAIN/api/v1/nodes/join \
  -H "Content-Type: application/json" \
  -d '{"node_name":"worker-node","capabilities":["coding","gpu","browser"],"endpoint":"http://worker:8787"}')
echo "  $JOIN_RESP" | python3 -m json.tool 2>/dev/null || echo "  $JOIN_RESP"

# --- Test 4: List nodes again (should see both) ---
echo ""
echo "[4/8] List nodes after join..."
NODES=$(curl -sf $MAIN/api/v1/nodes)
echo "  $NODES" | python3 -m json.tool 2>/dev/null || echo "  $NODES"

# --- Test 5: Submit a task ---
echo ""
echo "[5/8] Submit task to main..."
TASK_RESP=$(curl -sf -X POST $MAIN/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"title":"Test task: build Docker image","requires":["coding"]}')
echo "  $TASK_RESP" | python3 -m json.tool 2>/dev/null || echo "  $TASK_RESP"
TASK_ID=$(echo $TASK_RESP | python3 -c "import json,sys;print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
echo "  Task ID: $TASK_ID"

# --- Test 6: List tasks ---
echo ""
echo "[6/8] List tasks on main..."
TASKS=$(curl -sf $MAIN/api/v1/tasks)
echo "  $TASKS" | python3 -m json.tool 2>/dev/null || echo "  $TASKS"

# --- Test 7: Heartbeat from worker ---
echo ""
echo "[7/8] Worker sends heartbeat..."
HB_RESP=$(curl -sf -X POST $WORKER/api/v1/nodes/heartbeat \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node-worker"}')
echo "  $HB_RESP" | python3 -m json.tool 2>/dev/null || echo "  $HB_RESP"

# --- Test 8: Complete task ---
echo ""
echo "[8/8] Complete task..."
if [ -n "$TASK_ID" ]; then
  COMP_RESP=$(curl -sf -X POST $MAIN/api/v1/tasks/$TASK_ID/complete \
    -H "Content-Type: application/json" \
    -d '{"node_id":"node-main","result":"Docker image built successfully"}')
  echo "  $COMP_RESP" | python3 -m json.tool 2>/dev/null || echo "  $COMP_RESP"
else
  echo "  ⚠️  No task ID to complete"
fi

echo ""
echo "========================================="
echo "  All tests completed!"
echo "========================================="
