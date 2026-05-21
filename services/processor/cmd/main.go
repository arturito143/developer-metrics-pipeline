package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Event struct {
	EventID     string    `json:"event_id"`
	DeveloperID string    `json:"developer_id"`
	MetricType  string    `json:"metric_type"`
	Value       int       `json:"value"`
	Repository  string    `json:"repository"`
	Timestamp   time.Time `json:"timestamp"`
}

type ProcessedEvent struct {
	Event
	ProcessedAt time.Time `json:"processed_at"`
	ProcessorID string    `json:"processor_id"`
}

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

	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String("http://localstack:4566")
	})

	rawQueueURL := "http://localstack:4566/000000000000/raw-events"
	processedQueueURL := "http://localstack:4566/000000000000/processed-events"

	log.Println("processor started")

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down processor")
			return

		default:
			resp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(rawQueueURL),
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

				if !isValid(event) {
					log.Println("invalid event:", event.EventID)
					continue
				}

				processed := ProcessedEvent{
					Event:       event,
					ProcessedAt: time.Now(),
					ProcessorID: "processor-1",
				}

				body, _ := json.Marshal(processed)

				_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
					QueueUrl:    aws.String(processedQueueURL),
					MessageBody: aws.String(string(body)),
				})

				if err != nil {
					log.Println(err)
					continue
				}

				_, err = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      aws.String(rawQueueURL),
					ReceiptHandle: msg.ReceiptHandle,
				})

				if err != nil {
					log.Println(err)
				}

				log.Println("processed event:", event.EventID)
			}
		}
	}
}

func isValid(e Event) bool {
	if e.EventID == "" {
		return false
	}

	if e.DeveloperID == "" {
		return false
	}

	validTypes := map[string]bool{
		"commits":             true,
		"pull_requests":       true,
		"review_time_minutes": true,
	}

	if !validTypes[e.MetricType] {
		return false
	}

	if e.Value < 0 {
		return false
	}

	if e.MetricType == "review_time_minutes" && e.Value > 1440 {
		return false
	}

	if e.Timestamp.After(time.Now()) {
		return false
	}

	return true
}