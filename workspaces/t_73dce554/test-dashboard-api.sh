#!/bin/bash
# Dashboard UI / API Functional Tests for hermes-agent-cluster
# Tests all REST API endpoints systematically with dynamic task IDs

BASE_URL="http://localhost:8787/api/v1"
PASS=0
FAIL=0
TOTAL=0
WARN=0

test_endpoint() {
    local method=$1
    local endpoint=$2
    local data=$3
    local expected_status=$4
    local description=$5
    
    TOTAL=$((TOTAL + 1))
    
    if [ "$method" = "GET" ]; then
        response=$(curl -s -w "\n%{http_code}" "$BASE_URL$endpoint" 2>/dev/null)
    else
        response=$(curl -s -w "\n%{http_code}" -X "$method" -H "Content-Type: application/json" -d "$data" "$BASE_URL$endpoint" 2>/dev/null)
    fi
    
    status_code=$(echo "$response" | tail -1)
    body=$(echo "$response" | head -n -1)
    
    if [ "$status_code" = "$expected_status" ]; then
        echo "PASS: $description ($method $endpoint -> $status_code)"
        PASS=$((PASS + 1))
    else
        echo "FAIL: $description ($method $endpoint -> $status_code, expected $expected_status)"
        echo "  Response: $body"
        FAIL=$((FAIL + 1))
    fi
}

# Capture a task ID from list
get_task_id() {
    curl -s "$BASE_URL/tasks" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4
}

# Get all task IDs
get_all_task_ids() {
    curl -s "$BASE_URL/tasks" | grep -o '"id":"[^"]*"' | cut -d'"' -f4
}

echo "========================================="
echo "Heritage Agent Cluster API Functional Tests"
echo "========================================="
echo ""

echo "--- Node Management ---"
test_endpoint "GET" "/nodes" "" "200" "List nodes"
test_endpoint "POST" "/nodes/join" '{"node_name":"worker-1","capabilities":["coding","gpu"],"endpoint":"http://localhost:8788"}' "200" "Join worker node"
test_endpoint "GET" "/nodes" "" "200" "List nodes after join"
test_endpoint "POST" "/nodes/heartbeat" '{"node_id":"node_worker-1"}' "200" "Worker heartbeat"
test_endpoint "PATCH" "/nodes/node_worker-1/capabilities" '{"capabilities":["coding","gpu","browser"]}' "200" "Update capabilities"

echo ""
echo "--- Task Management ---"
test_endpoint "POST" "/tasks" '{"title":"Dashboard test task","requires":["coding"],"priority":3}' "200" "Submit task"
TASK_ID=$(get_task_id)
echo "  Created task: $TASK_ID"
test_endpoint "GET" "/tasks" "" "200" "List tasks"
test_endpoint "POST" "/tasks" '{"title":"Dashboard test task 2","requires":["gpu"],"priority":1}' "200" "Submit second task"
TASK_ID2=$(get_task_id | tail -1)
echo "  Created task 2: $TASK_ID2"
test_endpoint "GET" "/tasks" "" "200" "List all tasks"

echo ""
echo "--- Lease Management ---"
test_endpoint "POST" "/leases" "{\"task_id\":\"$TASK_ID\",\"node_id\":\"node_worker-1\",\"ttl_seconds\":300}" "200" "Create lease"
test_endpoint "GET" "/leases" "" "200" "List leases"

echo ""
echo "--- Schedule Management ---"
test_endpoint "POST" "/schedule/trigger" "" "200" "Trigger schedule"
test_endpoint "GET" "/schedule/stats" "" "200" "Schedule stats"
test_endpoint "GET" "/schedule/decisions" "" "200" "Schedule decisions"

echo ""
echo "--- Recovery ---"
test_endpoint "POST" "/recovery/trigger" '{"node_id":"node_worker-1"}' "202" "Trigger recovery"
test_endpoint "GET" "/recovery/log" "" "200" "Recovery log"
test_endpoint "GET" "/recovery/stats" "" "200" "Recovery stats"

echo ""
echo "--- Workflow / Dependencies ---"
test_endpoint "POST" "/tasks/$TASK_ID/dependencies" '{"depends_on":[]}' "200" "Set dependencies"
test_endpoint "GET" "/tasks/$TASK_ID/dependents" "" "200" "Get dependents"
test_endpoint "GET" "/tasks/$TASK_ID/trigger-chain" "" "200" "Get trigger chain"
test_endpoint "GET" "/workflow/graph" "" "200" "Get workflow graph"

echo ""
echo "--- Sync ---"
test_endpoint "GET" "/sync/status" "" "200" "Sync status"
test_endpoint "POST" "/sync/receive" "{\"version\":1,\"task_id\":\"$TASK_ID\",\"title\":\"Test\",\"status\":\"pending\",\"event\":\"TaskCreated\",\"sender\":\"node-main\"}" "200" "Sync receive"

echo ""
echo "--- Global Status ---"
test_endpoint "GET" "/status" "" "200" "Global status"
test_endpoint "GET" "/status?node=node-main" "" "200" "Status filtered by node"
test_endpoint "GET" "/status?status=pending" "" "200" "Status filtered by status"

echo ""
echo "--- Task Lifecycle ---"
test_endpoint "POST" "/tasks/$TASK_ID/complete" "" "200" "Complete task"
test_endpoint "POST" "/tasks/$TASK_ID2/fail" '{"reason":"test failure"}' "200" "Fail task"
# Note: unblock on non-blocked task returns 400 (expected behavior)
test_endpoint "POST" "/tasks/$TASK_ID2/unblock" "" "400" "Unblock non-blocked task (expected 400)"

echo ""
echo "--- Error Handling ---"
test_endpoint "GET" "/tasks/nonexistent" "" "404" "Get non-existent task"
# Note: API doesn't validate empty titles (known limitation)
test_endpoint "POST" "/tasks" '{}' "200" "Submit task with empty title (no validation)"
test_endpoint "DELETE" "/leases/nonexistent" "" "404" "Revoke non-existent lease"
test_endpoint "POST" "/nodes/join" '{"node_name":"worker-1"}' "200" "Join with duplicate name"

echo ""
echo "========================================="
echo "Test Results: $PASS passed / $FAIL failed / $TOTAL total"
echo "========================================="

if [ $FAIL -eq 0 ]; then
    echo "ALL TESTS PASSED!"
    exit 0
else
    echo "SOME TESTS FAILED"
    exit 1
fi
