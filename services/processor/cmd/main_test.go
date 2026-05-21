package main

import (
	"testing"
	"time"
)

func TestIsValid_ValidCommitsEvent(t *testing.T) {
	e := Event{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "dev-1",
		MetricType:  "commits",
		Value:       10,
		Repository:  "org/repo",
		Timestamp:   time.Now().Add(-1 * time.Hour),
	}
	if !IsValid(e) {
		t.Error("expected valid event")
	}
}

func TestIsValid_ValidPullRequestsEvent(t *testing.T) {
	e := Event{
		EventID:     "550e8400-e29b-41d4-a716-446655440001",
		DeveloperID: "dev-2",
		MetricType:  "pull_requests",
		Value:       5,
		Repository:  "org/repo",
		Timestamp:   time.Now().Add(-2 * time.Hour),
	}
	if !IsValid(e) {
		t.Error("expected valid event")
	}
}

func TestIsValid_ValidReviewTimeEvent(t *testing.T) {
	e := Event{
		EventID:     "550e8400-e29b-41d4-a716-446655440002",
		DeveloperID: "dev-3",
		MetricType:  "review_time_minutes",
		Value:       60,
		Repository:  "org/repo",
		Timestamp:   time.Now().Add(-3 * time.Hour),
	}
	if !IsValid(e) {
		t.Error("expected valid event")
	}
}

func TestIsValid_EmptyEventID(t *testing.T) {
	e := Event{
		EventID:     "",
		DeveloperID: "dev-1",
		MetricType:  "commits",
		Value:       10,
		Repository:  "org/repo",
		Timestamp:   time.Now().Add(-1 * time.Hour),
	}
	if IsValid(e) {
		t.Error("expected invalid: empty event_id")
	}
}

func TestIsValid_NonUUIDEventID(t *testing.T) {
	e := Event{
		EventID:     "not-a-uuid",
		DeveloperID: "dev-1",
		MetricType:  "commits",
		Value:       10,
		Repository:  "org/repo",
		Timestamp:   time.Now().Add(-1 * time.Hour),
	}
	if IsValid(e) {
		t.Error("expected invalid: non-UUID event_id")
	}
}

func TestIsValid_EmptyDeveloperID(t *testing.T) {
	e := Event{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "",
		MetricType:  "commits",
		Value:       10,
		Repository:  "org/repo",
		Timestamp:   time.Now().Add(-1 * time.Hour),
	}
	if IsValid(e) {
		t.Error("expected invalid: empty developer_id")
	}
}

func TestIsValid_InvalidMetricType(t *testing.T) {
	e := Event{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "dev-1",
		MetricType:  "invalid_type",
		Value:       10,
		Repository:  "org/repo",
		Timestamp:   time.Now().Add(-1 * time.Hour),
	}
	if IsValid(e) {
		t.Error("expected invalid: bad metric_type")
	}
}

func TestIsValid_NegativeValue(t *testing.T) {
	e := Event{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "dev-1",
		MetricType:  "commits",
		Value:       -1,
		Repository:  "org/repo",
		Timestamp:   time.Now().Add(-1 * time.Hour),
	}
	if IsValid(e) {
		t.Error("expected invalid: negative value")
	}
}

func TestIsValid_ReviewTimeExceeds1440(t *testing.T) {
	e := Event{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "dev-1",
		MetricType:  "review_time_minutes",
		Value:       1441,
		Repository:  "org/repo",
		Timestamp:   time.Now().Add(-1 * time.Hour),
	}
	if IsValid(e) {
		t.Error("expected invalid: review_time > 1440")
	}
}

func TestIsValid_FutureTimestamp(t *testing.T) {
	e := Event{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "dev-1",
		MetricType:  "commits",
		Value:       10,
		Repository:  "org/repo",
		Timestamp:   time.Now().Add(24 * time.Hour),
	}
	if IsValid(e) {
		t.Error("expected invalid: future timestamp")
	}
}

func TestIsValid_ZeroValue(t *testing.T) {
	e := Event{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "dev-1",
		MetricType:  "commits",
		Value:       0,
		Repository:  "org/repo",
		Timestamp:   time.Now().Add(-1 * time.Hour),
	}
	if !IsValid(e) {
		t.Error("expected valid: zero value is allowed")
	}
}

func TestIsValid_ReviewTimeExactly1440(t *testing.T) {
	e := Event{
		EventID:     "550e8400-e29b-41d4-a716-446655440000",
		DeveloperID: "dev-1",
		MetricType:  "review_time_minutes",
		Value:       1440,
		Repository:  "org/repo",
		Timestamp:   time.Now().Add(-1 * time.Hour),
	}
	if !IsValid(e) {
		t.Error("expected valid: 1440 is the max allowed")
	}
}
