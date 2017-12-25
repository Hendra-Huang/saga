package saga

import (
	"encoding/json"

	"github.com/Shopify/sarama"
)

type (
	kafkaStorageClient struct {
		producer sarama.SyncProducer
		consumer sarama.Consumer
	}
)

var _ StorageClient = new(kafkaStorageClient)

// New creates log storageClient based on Kafka.
func New(brokerAddrs []string, partitions, replicas int) (StorageClient, error) {
	producer, err := sarama.NewSyncProducer(brokerAddrs, nil)
	if err != nil {
		return nil, err
	}
	consumer, err := sarama.NewConsumer(brokerAddrs, nil)
	if err != nil {
		return nil, err
	}
	return &kafkaStorageClient{
		producer: producer,
		consumer: consumer,
	}, nil
}

func (s *kafkaStorageClient) Produce(topic string, event Event) (int32, int64, error) {
	encoded, err := json.Marshal(event)
	if err != nil {
		return 0, 0, err
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(encoded),
	}

	partition, offset, err := s.producer.SendMessage(msg)
	if err != nil {
		return 0, 0, err
	}

	return partition, offset, nil
}

func (s *kafkaStorageClient) Consume(topic string, offset int64) (<-chan Message, error) {
	partitionConsumer, err := s.consumer.ConsumePartition(topic, 0, offset)
	if err != nil {
		return nil, err
	}

	messages := make(chan Message)
	go func(partitionConsumer sarama.PartitionConsumer) {
		defer partitionConsumer.Close()
		for msg := range partitionConsumer.Messages() {
			messages <- Message{*msg}
		}
		close(messages)
	}(partitionConsumer)

	return messages, nil
}
