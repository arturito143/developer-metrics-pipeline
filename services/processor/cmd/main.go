package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

func init() {
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetOutput(os.Stdout)
}

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
	rawQueueURL := getEnv("RAW_QUEUE_URL", "http://localstack:4566/000000000000/raw-events")
	processedQueueURL := getEnv("PROCESSED_QUEUE_URL", "http://localstack:4566/000000000000/processed-events")

	workerCount := 5
	if v := os.Getenv("WORKER_COUNT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			workerCount = n
		}
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		log.WithError(err).Fatal("failed to load AWS config")
	}

	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(sqsEndpoint)
	})

	log.WithField("worker_count", workerCount).Info("processor started")

	msgCh := make(chan sqstypes.Message, workerCount*2)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for msg := range msgCh {
				processMessage(ctx, client, rawQueueURL, processedQueueURL, msg, workerID)
			}
		}(i)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(msgCh)
				return
			default:
				resp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
					QueueUrl:            aws.String(rawQueueURL),
					MaxNumberOfMessages: 5,
					WaitTimeSeconds:     5,
				})
				if err != nil {
					if ctx.Err() != nil {
						close(msgCh)
						return
					}
					log.WithError(err).Error("failed to receive messages")
					time.Sleep(1 * time.Second)
					continue
				}
				for _, msg := range resp.Messages {
					msgCh <- msg
				}
			}
		}
	}()

	<-ctx.Done()
	log.Info("shutting down processor, draining workers...")
	wg.Wait()
	log.Info("processor stopped")
}

func processMessage(ctx context.Context, client *sqs.Client, rawQueueURL, processedQueueURL string, msg sqstypes.Message, workerID int) {
	var event Event
	if err := json.Unmarshal([]byte(*msg.Body), &event); err != nil {
		log.WithError(err).Error("failed to unmarshal event")
		deleteMsg(ctx, client, rawQueueURL, msg.ReceiptHandle)
		return
	}

	logger := log.WithField("event_id", event.EventID)

	if !IsValid(event) {
		logger.Warn("invalid event, skipping")
		deleteMsg(ctx, client, rawQueueURL, msg.ReceiptHandle)
		return
	}

	processed := ProcessedEvent{
		Event:       event,
		ProcessedAt: time.Now(),
		ProcessorID: fmt.Sprintf("processor-worker-%d", workerID),
	}

	body, _ := json.Marshal(processed)

	if err := sendWithRetry(ctx, client, processedQueueURL, string(body)); err != nil {
		logger.WithError(err).Error("failed to send to processed-events after retries")
		return
	}

	deleteMsg(ctx, client, rawQueueURL, msg.ReceiptHandle)
	logger.Info("event processed")
}

func sendWithRetry(ctx context.Context, client *sqs.Client, queueURL, body string) error {
	maxRetries := 3
	for i := 0; i <= maxRetries; i++ {
		_, err := client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    aws.String(queueURL),
			MessageBody: aws.String(body),
		})
		if err == nil {
			return nil
		}
		if i == maxRetries {
			return err
		}
		backoff := time.Duration(math.Pow(2, float64(i))) * 100 * time.Millisecond
		log.WithField("retry", i+1).WithError(err).Warn("send failed, retrying")
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return nil
}

func deleteMsg(ctx context.Context, client *sqs.Client, queueURL string, receiptHandle *string) {
	_, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		log.WithError(err).Error("failed to delete message")
	}
}

// IsValid checks if an event passes all validation rules.
func IsValid(e Event) bool {
	if e.EventID == "" {
		return false
	}
	if _, err := uuid.Parse(e.EventID); err != nil {
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
