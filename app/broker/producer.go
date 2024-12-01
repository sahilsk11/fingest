package broker

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const Topic = "DEFAULT"

type Producer interface {
	Publish(eventType string, payload interface{}) error
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

func (p *kafkaProducer) Publish(eventType string, payload interface{}) error {
	bytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}
	topic := Topic
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Headers: []kafka.Header{
			{
				Key:   "event",
				Value: []byte(eventType),
			},
		},
		Value: bytes,
	}
	delivery_chan := make(chan kafka.Event, 10000)

	err = p.producer.Produce(msg, delivery_chan)
	if err != nil {
		return fmt.Errorf("failed to send message to kafka: %w", err)
	}
	e := <-delivery_chan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
	close(delivery_chan)

	x := p.producer.Flush(1000)
	fmt.Println(x)
	fmt.Println("def pubbed")

	return nil
}
