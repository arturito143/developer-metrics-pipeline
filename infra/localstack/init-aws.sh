#!/bin/bash
awslocal sqs create-queue --queue-name raw-events
awslocal sqs create-queue --queue-name processed-events
