#!/bin/bash

for i in {1..20}
do
docker exec -i dev_metrics_pipeline_complete-localstack-1 awslocal sqs send-message \
--queue-url http://localhost:4566/000000000000/raw-events \
--message-body "{\"event_id\":\"$i\",\"developer_id\":\"dev-1\",\"metric_type\":\"commits\",\"value\":$i,\"repository\":\"org/repo\",\"timestamp\":\"2026-04-15T10:30:00Z\"}"
done