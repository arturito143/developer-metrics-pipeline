package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Event struct {
	EventID     string    `json:"event_id"`
	DeveloperID string    `json:"developer_id"`
	MetricType  string    `json:"metric_type"`
	Value       int       `json:"value"`
	Repository  string    `json:"repository"`
	Timestamp   time.Time `json:"timestamp"`
	ProcessedAt time.Time `json:"processed_at"`
	ProcessorID string    `json:"processor_id"`
}

type Summary struct {
	DeveloperID           string `json:"developer_id"`
	TotalCommits          int    `json:"total_commits"`
	TotalPullRequests     int    `json:"total_pull_requests"`
	AvgReviewTimeMinutes  int    `json:"avg_review_time_minutes"`
	EventsProcessed       int    `json:"events_processed"`
}

var (
	summaries = map[string]*Summary{}
	mutex     sync.Mutex
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)

	if err != nil {
		log.Fatal(err)
	}

	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String("http://localstack:4566")
	})

	dbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://localstack:4566")
	})

	go consumeQueue(ctx, sqsClient, dbClient)

	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/metrics/dev-1/summary", summaryHandler)

	log.Println("aggregator running on 8080")

	http.ListenAndServe(":8080", nil)
}

func consumeQueue(ctx context.Context, sqsClient *sqs.Client, dbClient *dynamodb.Client) {
	queueURL := "http://localstack:4566/000000000000/processed-events"

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
				log.Println(err)
				continue
			}

			for _, msg := range resp.Messages {
				var event Event

				err := json.Unmarshal([]byte(*msg.Body), &event)
				if err != nil {
					log.Println(err)
					continue
				}

				saveEvent(ctx, dbClient, event)

				updateSummary(event)

				_, _ = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      aws.String(queueURL),
					ReceiptHandle: msg.ReceiptHandle,
				})

				log.Println("aggregated event:", event.EventID)
			}
		}
	}
}

func saveEvent(ctx context.Context, db *dynamodb.Client, event Event) {
	item, _ := attributevalue.MarshalMap(event)

	_, err := db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("events"),
		Item:      item,
	})

	if err != nil {
		log.Println(err)
	}
}

func updateSummary(event Event) {
	mutex.Lock()
	defer mutex.Unlock()

	summary, exists := summaries[event.DeveloperID]

	if !exists {
		summary = &Summary{
			DeveloperID: event.DeveloperID,
		}
		summaries[event.DeveloperID] = summary
	}

	switch event.MetricType {
	case "commits":
		summary.TotalCommits += event.Value

	case "pull_requests":
		summary.TotalPullRequests += event.Value

	case "review_time_minutes":
		summary.AvgReviewTimeMinutes = event.Value
	}

	summary.EventsProcessed++
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok"}`))
}

func summaryHandler(w http.ResponseWriter, r *http.Request) {
	mutex.Lock()
	defer mutex.Unlock()

	summary := summaries["dev-1"]

	if summary == nil {
		summary = &Summary{
			DeveloperID: "dev-1",
		}
	}

	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(summary)
}