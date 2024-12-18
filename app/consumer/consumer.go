package consumer

import (
	"context"
	"fmt"
	"time"

	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sahilsk11/fingest/app/broker"
	"github.com/sahilsk11/fingest/app/cmd"
)

type Consumer interface {
	Close() error
	Start(context.Context) error
}

type consumerHandler struct {
	consumer        *kafka.Consumer
	appDependencies cmd.Dependencies
}

type Event struct {
	EventType string
	Payload   []byte
	Timestamp time.Time
}

func NewConsumer(host string, port int, appDependencies *cmd.Dependencies) (Consumer, error) {
	service := fmt.Sprintf("%s:%d", host, port)
	groupId := "go-default-group-id"
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": service,
		"group.id":          groupId,
		// "auto.offset.reset": "smallest",
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	return &consumerHandler{
		consumer:        consumer,
		appDependencies: *appDependencies,
	}, nil
}

func (c *consumerHandler) Close() error {
	return c.consumer.Close()
}

func (c *consumerHandler) handleEvent(event Event) error {
	ignoreJob := func([]byte, time.Time) error {
		return nil
	}

	handlers := map[string]func([]byte, time.Time) error{
		"IMPORT_RUN_STATUS_UPDATED": c.importRunStatusUpdated,
		"FILE_IMPORT_COMPLETED":     c.fileImportCompleted,

		// ignore these jobs
		"FILE_UPLOADED": ignoreJob,
	}
	if _, ok := handlers[event.EventType]; !ok {
		fmt.Printf("No handler found for event type %s\n", event.EventType)
		return nil
	}

	err := handlers[event.EventType](event.Payload, event.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to handle event: %w", err)
	}

	return nil
}

func (c *consumerHandler) Start(ctx context.Context) error {
	defer func() {
		err := c.Close()
		if err != nil {
			log.Fatalf("failed to close consumer: %v", err)
		}
		log.Println("consumer closed")
	}()

	err := c.consumer.SubscribeTopics([]string{broker.Topic}, nil)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic: %w", err)
	}

	fmt.Println("consumer started")

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			ev := c.consumer.Poll(1000)
			switch e := ev.(type) {
			case *kafka.Message:
				eventName := ""
				for _, h := range e.Headers {
					if h.Key == "event" {
						eventName = string(h.Value)
					}
				}
				event := Event{
					EventType: eventName,
					Payload:   e.Value,
					Timestamp: e.Timestamp,
				}
				err = c.handleEvent(event)
				if err != nil {
					log.Println(fmt.Errorf("failed to handle event: %w", err))
				}
				_, err = c.consumer.Commit()
				if err != nil {
					return fmt.Errorf("failed to commit consumer: %w", err)
				}
			case kafka.Error:
				return fmt.Errorf("failed to poll consumer: %w", e)
			case nil:
				continue
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
}
