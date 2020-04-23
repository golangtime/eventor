### Shared-service-package

Пакет для упрощения создания сервиса, который читает сообщения из очереди
NATSStreaming

# Базовое применение

```go

import (
    "log"

    "eventor/pkg/service"
)

type Handler struct {
}

func (Handler) Handle(m *stan.Msg) {
    log.Println("handle message", m)
}

func main() {

    logger := log.New(os.Stdout, "", log.Lstdflags)

    service, err := service.NewEventBatchStreamingService(service.NatsStreamingConfig{
		URL:       "nats://localhost:4222",
		ClusterID: "test-cluster",
		ClientID:  "test-client-1",
		Subscriptions: []service.SubscriptionConfig{
			{
				Name:        "logs",
				QueueGroup:  "logs",
				DurableName: "logs",
				Limit:       1000,
				Timeout:     5,
			},
		},
    }, logger)

    if err != nil {
        ...
    }

    defer service.Close()

    service.Handle("logs", Handler{})

    if err := service.Start(); err != nil {
        ...
    }
}
```