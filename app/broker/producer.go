package broker

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const Topic = "DEFAULT"

type Producer interface {
	Publish(topic string, payload interface{}) error
}

func NewProducer(host string, port int) (Producer, error) {
	server := fmt.Sprintf("%s:%d", host, port)
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": server,
		// "client.id":         "myProducer",
		// "acks":              "all",
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}
	return &kafkaProducer{
		producer: p,
	}, nil
}

type kafkaProducer struct {
	producer *kafka.Producer
}

func (p *kafkaProducer) Publish(topic string, payload interface{}) error {
	bytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: bytes,
	}
	err = p.producer.Produce(msg, nil)
	if err != nil {
		return fmt.Errorf("failed to send message to kafka: %w", err)
	}

	return nil
}
