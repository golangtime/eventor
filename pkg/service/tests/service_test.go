package tests

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/nats-io/stan.go"
	"eventor/pkg/service"
)

type MockStreamingHandler struct {
	messages []*stan.Msg
}

func (h *MockStreamingHandler) Handle(messages []*stan.Msg) error {
	h.messages = append(h.messages, messages...)
	return nil
}

var testLogger = log.New(os.Stdout, "", log.LstdFlags)

func TestServiceBasicSubscription(t *testing.T) {

	handler := &MockStreamingHandler{}

	service, err := service.NewEventBatchStreamingService(service.NatsStreamingConfig{
		URL:       "nats://localhost:4222",
		ClusterID: "test-cluster",
		ClientID:  "test-client-1",
		Subscriptions: []service.SubscriptionConfig{
			{
				Name:        "logs",
				QueueGroup:  "logs",
				DurableName: "logs-durable",
				Limit:       1,
				Timeout:     1,
			},
		},
	}, testLogger)

	if err != nil {
		t.Errorf("errors must be nil, was: %v", err)
		return
	}

	defer service.Close()

	if err := service.Handle("logs", handler); err != nil {
		t.Errorf("errors must be nil, was: %v", err)
		return
	}

	if err := service.Start(); err != nil {
		t.Errorf("errors must be nil, was: %v", err)
		return
	}

	natsConn := service.Conn()

	for i := 0; i < 1; i++ {
		_ = natsConn.Publish("logs", []byte(fmt.Sprintf(`
			{
				"ID": %d,
				"Name": "Msg-1",
			}
		`, i)))
	}

	time.Sleep(time.Second * 2)

	fmt.Println("check events", len(handler.messages))
	if len(handler.messages) != 1 {
		t.Errorf("event storage must store 1 element, was: %d", len(handler.messages))
	}
}

func TestServiceLimitOddNumberOfEvents(t *testing.T) {

	handler := &MockStreamingHandler{}

	service, err := service.NewEventBatchStreamingService(service.NatsStreamingConfig{
		URL:       "nats://localhost:4222",
		ClusterID: "test-cluster",
		ClientID:  "test-client-2",
		Subscriptions: []service.SubscriptionConfig{
			{
				Name:        "logs",
				QueueGroup:  "logs",
				DurableName: "logs-durable",
				Limit:       2,
				Timeout:     2,
			},
		},
	}, testLogger)

	if err != nil {
		t.Errorf("errors must be nil, was: %v", err)
		return
	}

	defer service.Close()

	if err := service.Handle("logs", handler); err != nil {
		t.Errorf("errors must be nil, was: %v", err)
		return
	}

	if err := service.Start(); err != nil {
		t.Errorf("errors must be nil, was: %v", err)
		return
	}

	natsConn := service.Conn()

	for i := 0; i < 3; i++ {
		_ = natsConn.Publish("logs", []byte(fmt.Sprintf(`
			{
				"ID": %d,
				"Name": "Msg-2-%d"
			}
		`, i, i+1)))
	}

	time.Sleep(time.Second * 3)

	if len(handler.messages) != 2 {
		t.Errorf("event storage must store 2 element, was: %d", len(handler.messages))
	}
}

func TestServiceLimitEvenNumberOfEvents(t *testing.T) {

	handler := &MockStreamingHandler{}

	service, err := service.NewEventBatchStreamingService(service.NatsStreamingConfig{
		URL:       "nats://localhost:4222",
		ClusterID: "test-cluster",
		ClientID:  "test-client-3",
		Subscriptions: []service.SubscriptionConfig{
			{
				Name:        "logs",
				QueueGroup:  "logs",
				DurableName: "logs-durable",
				Limit:       2,
				Timeout:     1,
			},
		},
	}, testLogger)

	if err != nil {
		t.Errorf("errors must be nil, was: %v", err)
		return
	}

	defer service.Close()

	if err := service.Handle("logs", handler); err != nil {
		t.Errorf("errors must be nil, was: %v", err)
		return
	}

	if err := service.Start(); err != nil {
		t.Errorf("errors must be nil, was: %v", err)
		return
	}

	natsConn := service.Conn()

	for i := 0; i < 4; i++ {
		_ = natsConn.Publish("logs", []byte(fmt.Sprintf(`
			{
				"ID": %d,
				"Name": "Msg-3-%d"
			}
		`, i, i+1)))
	}

	time.Sleep(time.Second * 3)

	if len(handler.messages) != 4 {
		t.Errorf("event storage must store 4 element, was: %d", len(handler.messages))
	}

	fmt.Println(handler.messages)
}
