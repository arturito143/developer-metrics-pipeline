#!/bin/bash
set -e

echo "Creating DLQ queues..."
awslocal sqs create-queue --queue-name raw-events-dlq
awslocal sqs create-queue --queue-name processed-events-dlq

RAW_DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/raw-events-dlq \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

PROCESSED_DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/processed-events-dlq \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

echo "Creating main queues with RedrivePolicy..."
awslocal sqs create-queue --queue-name raw-events \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${RAW_DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

awslocal sqs create-queue --queue-name processed-events \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${PROCESSED_DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"}"

echo "Creating DynamoDB tables..."
awslocal dynamodb create-table \
  --table-name events \
  --attribute-definitions AttributeName=event_id,AttributeType=S \
  --key-schema AttributeName=event_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

awslocal dynamodb create-table \
  --table-name developer_summary \
  --attribute-definitions AttributeName=developer_id,AttributeType=S \
  --key-schema AttributeName=developer_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

echo "LocalStack initialization complete."
