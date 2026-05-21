package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamotypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func init() {
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetOutput(os.Stdout)
}

type Event struct {
	EventID     string `json:"event_id" dynamodbav:"event_id"`
	DeveloperID string `json:"developer_id" dynamodbav:"developer_id"`
	MetricType  string `json:"metric_type" dynamodbav:"metric_type"`
	Value       int    `json:"value" dynamodbav:"value"`
	Repository  string `json:"repository" dynamodbav:"repository"`
	Timestamp   string `json:"timestamp" dynamodbav:"timestamp"`
	ProcessedAt string `json:"processed_at" dynamodbav:"processed_at"`
	ProcessorID string `json:"processor_id" dynamodbav:"processor_id"`
}

type Summary struct {
	DeveloperID          string `json:"developer_id" dynamodbav:"developer_id"`
	TotalCommits         int    `json:"total_commits" dynamodbav:"total_commits"`
	TotalPullRequests    int    `json:"total_pull_requests" dynamodbav:"total_pull_requests"`
	AvgReviewTimeMinutes int    `json:"avg_review_time_minutes" dynamodbav:"avg_review_time_minutes"`
	EventsProcessed      int    `json:"events_processed" dynamodbav:"events_processed"`
	ReviewTimeSum        int    `json:"review_time_sum" dynamodbav:"review_time_sum"`
	ReviewTimeCount      int    `json:"review_time_count" dynamodbav:"review_time_count"`
	LastActivity         string `json:"last_activity" dynamodbav:"last_activity"`
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	region := getEnv("AWS_REGION", "us-east-1")
	sqsEndpoint := getEnv("SQS_ENDPOINT", "http://localstack:4566")
	dynamoEndpoint := getEnv("DYNAMODB_ENDPOINT", "http://localstack:4566")
	queueURL := getEnv("PROCESSED_QUEUE_URL", "http://localstack:4566/000000000000/processed-events")

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		log.WithError(err).Fatal("failed to load AWS config")
	}

	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(sqsEndpoint)
	})

	dbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(dynamoEndpoint)
	})

	waitForQueue(ctx, sqsClient, queueURL)

	go consumeQueue(ctx, sqsClient, dbClient, queueURL)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		healthHandler(w, r, sqsClient, dbClient, queueURL)
	})
	mux.HandleFunc("/metrics/", func(w http.ResponseWriter, r *http.Request) {
		metricsRouter(w, r, dbClient)
	})

	server := &http.Server{Addr: ":8080", Handler: mux}

	go func() {
		log.Info("aggregator running on 8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.WithError(err).Fatal("server error")
		}
	}()

	<-ctx.Done()
	log.Info("shutting down aggregator...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server.Shutdown(shutdownCtx)

	log.Info("aggregator stopped")
}

func consumeQueue(ctx context.Context, sqsClient *sqs.Client, dbClient *dynamodb.Client, queueURL string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			resp, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(queueURL),
				MaxNumberOfMessages: 5,
				WaitTimeSeconds:     5,
			})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.WithError(err).Error("failed to receive messages")
				time.Sleep(1 * time.Second)
				continue
			}

			for _, msg := range resp.Messages {
				var event Event
				if err := json.Unmarshal([]byte(*msg.Body), &event); err != nil {
					log.WithError(err).Error("failed to unmarshal event")
					continue
				}

				logger := log.WithField("event_id", event.EventID)

				if isDuplicate(ctx, dbClient, event.EventID) {
					logger.Warn("duplicate event, skipping")
					deleteMessage(ctx, sqsClient, queueURL, msg.ReceiptHandle)
					continue
				}

				saveEvent(ctx, dbClient, event)
				UpdateSummary(ctx, dbClient, event)

				deleteMessage(ctx, sqsClient, queueURL, msg.ReceiptHandle)
				logger.Info("event aggregated")
			}
		}
	}
}

func isDuplicate(ctx context.Context, db *dynamodb.Client, eventID string) bool {
	result, err := db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("events"),
		Key: map[string]dynamotypes.AttributeValue{
			"event_id": &dynamotypes.AttributeValueMemberS{Value: eventID},
		},
		ProjectionExpression: aws.String("event_id"),
	})
	if err != nil {
		log.WithError(err).Error("failed to check idempotency")
		return false
	}
	return result.Item != nil
}

func saveEvent(ctx context.Context, db *dynamodb.Client, event Event) {
	item, err := attributevalue.MarshalMap(event)
	if err != nil {
		log.WithError(err).Error("failed to marshal event")
		return
	}

	_, err = db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("events"),
		Item:      item,
	})
	if err != nil {
		log.WithError(err).Error("failed to save event")
	}
}

// UpdateSummary loads summary from DynamoDB, updates it, and saves it back.
func UpdateSummary(ctx context.Context, db *dynamodb.Client, event Event) {
	summary := loadSummary(ctx, db, event.DeveloperID)

	switch event.MetricType {
	case "commits":
		summary.TotalCommits += event.Value
	case "pull_requests":
		summary.TotalPullRequests += event.Value
	case "review_time_minutes":
		summary.ReviewTimeSum += event.Value
		summary.ReviewTimeCount++
		summary.AvgReviewTimeMinutes = summary.ReviewTimeSum / summary.ReviewTimeCount
	}

	summary.EventsProcessed++

	if event.Timestamp > summary.LastActivity {
		summary.LastActivity = event.Timestamp
	}

	saveSummary(ctx, db, summary)
}

func loadSummary(ctx context.Context, db *dynamodb.Client, developerID string) Summary {
	result, err := db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("developer_summary"),
		Key: map[string]dynamotypes.AttributeValue{
			"developer_id": &dynamotypes.AttributeValueMemberS{Value: developerID},
		},
	})
	if err != nil || result.Item == nil {
		return Summary{DeveloperID: developerID}
	}

	var summary Summary
	if err := attributevalue.UnmarshalMap(result.Item, &summary); err != nil {
		return Summary{DeveloperID: developerID}
	}
	return summary
}

func saveSummary(ctx context.Context, db *dynamodb.Client, summary Summary) {
	item, err := attributevalue.MarshalMap(summary)
	if err != nil {
		log.WithError(err).Error("failed to marshal summary")
		return
	}

	_, err = db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("developer_summary"),
		Item:      item,
	})
	if err != nil {
		log.WithError(err).Error("failed to save summary")
	}
}

func waitForQueue(ctx context.Context, client *sqs.Client, queueURL string) {
	for {
		_, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(queueURL),
			AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
		})
		if err == nil {
			log.WithField("queue", queueURL).Info("queue is ready")
			return
		}
		log.WithField("queue", queueURL).Info("waiting for queue to be created...")
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func deleteMessage(ctx context.Context, client *sqs.Client, queueURL string, receiptHandle *string) {
	_, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		log.WithError(err).Error("failed to delete message")
	}
}

func metricsRouter(w http.ResponseWriter, r *http.Request, db *dynamodb.Client) {
	path := strings.TrimPrefix(r.URL.Path, "/metrics/")
	parts := strings.Split(path, "/")

	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}

	developerID := parts[0]

	if len(parts) == 2 && parts[1] == "summary" {
		summaryHandler(w, r, db, developerID)
		return
	}

	if len(parts) == 1 {
		eventsHandler(w, r, db, developerID)
		return
	}

	http.NotFound(w, r)
}

func summaryHandler(w http.ResponseWriter, r *http.Request, db *dynamodb.Client, developerID string) {
	summary := loadSummary(r.Context(), db, developerID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(summary)
}

func eventsHandler(w http.ResponseWriter, r *http.Request, db *dynamodb.Client, developerID string) {
	result, err := db.Scan(r.Context(), &dynamodb.ScanInput{
		TableName:        aws.String("events"),
		FilterExpression: aws.String("developer_id = :devId"),
		ExpressionAttributeValues: map[string]dynamotypes.AttributeValue{
			":devId": &dynamotypes.AttributeValueMemberS{Value: developerID},
		},
	})
	if err != nil {
		log.WithError(err).Error("failed to query events")
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	var events []Event
	if err := attributevalue.UnmarshalListOfMaps(result.Items, &events); err != nil {
		log.WithError(err).Error("failed to unmarshal events")
		http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

func healthHandler(w http.ResponseWriter, r *http.Request, sqsClient *sqs.Client, dbClient *dynamodb.Client, queueURL string) {
	ctx := r.Context()

	_, sqsErr := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
	})

	_, dbErr := dbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String("events"),
	})

	status := "ok"
	details := map[string]string{
		"sqs":      "connected",
		"dynamodb": "connected",
	}
	httpCode := http.StatusOK

	if sqsErr != nil {
		status = "degraded"
		details["sqs"] = fmt.Sprintf("error: %v", sqsErr)
		httpCode = http.StatusServiceUnavailable
	}
	if dbErr != nil {
		status = "degraded"
		details["dynamodb"] = fmt.Sprintf("error: %v", dbErr)
		httpCode = http.StatusServiceUnavailable
	}

	resp := map[string]interface{}{
		"status":  status,
		"details": details,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpCode)
	json.NewEncoder(w).Encode(resp)
}
