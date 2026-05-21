#!/bin/bash

LOCALSTACK_CONTAINER="developer-metrics-pipeline-localstack-1"
QUEUE_URL="http://localhost:4566/000000000000/raw-events"

send_message() {
  local body="$1"
  local label="$2"
  docker exec -i "$LOCALSTACK_CONTAINER" awslocal sqs send-message \
    --queue-url "$QUEUE_URL" \
    --message-body "$body" > /dev/null 2>&1
  echo "Sent: $label"
}

echo "=== Sending valid messages ==="

DEVELOPERS=("dev-1" "dev-2" "dev-3" "dev-4")
METRIC_TYPES=("commits" "pull_requests" "review_time_minutes")
REPOS=("org/api" "org/frontend" "org/infra" "org/docs")

DUPLICATE_UUID="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

for i in {1..20}; do
  UUID=$(cat /proc/sys/kernel/random/uuid)
  DEV=${DEVELOPERS[$((RANDOM % ${#DEVELOPERS[@]}))]}
  METRIC=${METRIC_TYPES[$((RANDOM % ${#METRIC_TYPES[@]}))]}
  REPO=${REPOS[$((RANDOM % ${#REPOS[@]}))]}

  if [ "$METRIC" = "review_time_minutes" ]; then
    VALUE=$((RANDOM % 120 + 1))
  else
    VALUE=$((RANDOM % 50 + 1))
  fi

  if [ "$i" -eq 1 ]; then
    UUID="$DUPLICATE_UUID"
  fi

  BODY="{\"event_id\":\"$UUID\",\"developer_id\":\"$DEV\",\"metric_type\":\"$METRIC\",\"value\":$VALUE,\"repository\":\"$REPO\",\"timestamp\":\"2026-04-15T10:30:00Z\"}"
  send_message "$BODY" "valid #$i ($DEV, $METRIC, value=$VALUE)"
done

echo ""
echo "=== Sending invalid messages ==="

send_message '{"event_id":"","developer_id":"dev-1","metric_type":"commits","value":5,"repository":"org/api","timestamp":"2026-04-15T10:30:00Z"}' \
  "invalid #1 (empty event_id)"

send_message '{"event_id":"not-a-uuid","developer_id":"dev-1","metric_type":"commits","value":5,"repository":"org/api","timestamp":"2026-04-15T10:30:00Z"}' \
  "invalid #2 (non-UUID event_id)"

send_message "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"developer_id\":\"dev-1\",\"metric_type\":\"invalid_type\",\"value\":5,\"repository\":\"org/api\",\"timestamp\":\"2026-04-15T10:30:00Z\"}" \
  "invalid #3 (invalid metric_type)"

send_message "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"developer_id\":\"dev-1\",\"metric_type\":\"commits\",\"value\":5,\"repository\":\"org/api\",\"timestamp\":\"2099-12-31T23:59:59Z\"}" \
  "invalid #4 (future timestamp)"

echo ""
echo "=== Sending duplicate messages ==="

send_message "{\"event_id\":\"$DUPLICATE_UUID\",\"developer_id\":\"dev-1\",\"metric_type\":\"commits\",\"value\":99,\"repository\":\"org/api\",\"timestamp\":\"2026-04-15T10:30:00Z\"}" \
  "duplicate #1 (same event_id as valid #1)"

send_message "{\"event_id\":\"$DUPLICATE_UUID\",\"developer_id\":\"dev-2\",\"metric_type\":\"pull_requests\",\"value\":10,\"repository\":\"org/frontend\",\"timestamp\":\"2026-04-15T10:30:00Z\"}" \
  "duplicate #2 (same event_id as valid #1)"

echo ""
echo "=== Seed complete: 20 valid + 4 invalid + 2 duplicates ==="
