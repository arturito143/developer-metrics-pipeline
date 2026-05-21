package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
)

func TestUpdateSummary_Commits(t *testing.T) {
	summary := Summary{DeveloperID: "dev-1"}
	event := Event{
		EventID:     "aaa-bbb",
		DeveloperID: "dev-1",
		MetricType:  "commits",
		Value:       10,
		Timestamp:   "2026-04-15T10:30:00Z",
	}

	applySummaryUpdate(&summary, event)

	if summary.TotalCommits != 10 {
		t.Errorf("expected TotalCommits=10, got %d", summary.TotalCommits)
	}
	if summary.EventsProcessed != 1 {
		t.Errorf("expected EventsProcessed=1, got %d", summary.EventsProcessed)
	}
}

func TestUpdateSummary_PullRequests(t *testing.T) {
	summary := Summary{DeveloperID: "dev-1"}
	event := Event{
		EventID:     "aaa-bbb",
		DeveloperID: "dev-1",
		MetricType:  "pull_requests",
		Value:       3,
		Timestamp:   "2026-04-15T10:30:00Z",
	}

	applySummaryUpdate(&summary, event)

	if summary.TotalPullRequests != 3 {
		t.Errorf("expected TotalPullRequests=3, got %d", summary.TotalPullRequests)
	}
}

func TestUpdateSummary_ReviewTimeAverage(t *testing.T) {
	summary := Summary{DeveloperID: "dev-1"}
	events := []Event{
		{EventID: "1", DeveloperID: "dev-1", MetricType: "review_time_minutes", Value: 60, Timestamp: "2026-04-15T10:00:00Z"},
		{EventID: "2", DeveloperID: "dev-1", MetricType: "review_time_minutes", Value: 120, Timestamp: "2026-04-15T11:00:00Z"},
		{EventID: "3", DeveloperID: "dev-1", MetricType: "review_time_minutes", Value: 90, Timestamp: "2026-04-15T12:00:00Z"},
	}

	for _, e := range events {
		applySummaryUpdate(&summary, e)
	}

	expectedAvg := (60 + 120 + 90) / 3
	if summary.AvgReviewTimeMinutes != expectedAvg {
		t.Errorf("expected AvgReviewTimeMinutes=%d, got %d", expectedAvg, summary.AvgReviewTimeMinutes)
	}
	if summary.ReviewTimeCount != 3 {
		t.Errorf("expected ReviewTimeCount=3, got %d", summary.ReviewTimeCount)
	}
}

func TestUpdateSummary_LastActivity(t *testing.T) {
	summary := Summary{DeveloperID: "dev-1"}
	events := []Event{
		{EventID: "1", DeveloperID: "dev-1", MetricType: "commits", Value: 5, Timestamp: "2026-04-15T10:00:00Z"},
		{EventID: "2", DeveloperID: "dev-1", MetricType: "commits", Value: 3, Timestamp: "2026-04-16T10:00:00Z"},
		{EventID: "3", DeveloperID: "dev-1", MetricType: "commits", Value: 7, Timestamp: "2026-04-14T10:00:00Z"},
	}

	for _, e := range events {
		applySummaryUpdate(&summary, e)
	}

	if summary.LastActivity != "2026-04-16T10:00:00Z" {
		t.Errorf("expected LastActivity=2026-04-16T10:00:00Z, got %s", summary.LastActivity)
	}
}

func TestUpdateSummary_MixedMetrics(t *testing.T) {
	summary := Summary{DeveloperID: "dev-1"}
	events := []Event{
		{EventID: "1", DeveloperID: "dev-1", MetricType: "commits", Value: 10, Timestamp: "2026-04-15T10:00:00Z"},
		{EventID: "2", DeveloperID: "dev-1", MetricType: "pull_requests", Value: 2, Timestamp: "2026-04-15T11:00:00Z"},
		{EventID: "3", DeveloperID: "dev-1", MetricType: "review_time_minutes", Value: 30, Timestamp: "2026-04-15T12:00:00Z"},
		{EventID: "4", DeveloperID: "dev-1", MetricType: "commits", Value: 5, Timestamp: "2026-04-15T13:00:00Z"},
	}

	for _, e := range events {
		applySummaryUpdate(&summary, e)
	}

	if summary.TotalCommits != 15 {
		t.Errorf("expected TotalCommits=15, got %d", summary.TotalCommits)
	}
	if summary.TotalPullRequests != 2 {
		t.Errorf("expected TotalPullRequests=2, got %d", summary.TotalPullRequests)
	}
	if summary.AvgReviewTimeMinutes != 30 {
		t.Errorf("expected AvgReviewTimeMinutes=30, got %d", summary.AvgReviewTimeMinutes)
	}
	if summary.EventsProcessed != 4 {
		t.Errorf("expected EventsProcessed=4, got %d", summary.EventsProcessed)
	}
}

func TestMarshalUnmarshalSummary(t *testing.T) {
	original := Summary{
		DeveloperID:          "dev-1",
		TotalCommits:         100,
		TotalPullRequests:    20,
		AvgReviewTimeMinutes: 45,
		EventsProcessed:      50,
		ReviewTimeSum:        900,
		ReviewTimeCount:      20,
		LastActivity:         "2026-04-15T10:00:00Z",
	}

	item, err := attributevalue.MarshalMap(original)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var result Summary
	if err := attributevalue.UnmarshalMap(item, &result); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if result.DeveloperID != original.DeveloperID {
		t.Errorf("DeveloperID mismatch: %s != %s", result.DeveloperID, original.DeveloperID)
	}
	if result.TotalCommits != original.TotalCommits {
		t.Errorf("TotalCommits mismatch: %d != %d", result.TotalCommits, original.TotalCommits)
	}
	if result.AvgReviewTimeMinutes != original.AvgReviewTimeMinutes {
		t.Errorf("AvgReviewTimeMinutes mismatch: %d != %d", result.AvgReviewTimeMinutes, original.AvgReviewTimeMinutes)
	}
}

func applySummaryUpdate(summary *Summary, event Event) {
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
}
