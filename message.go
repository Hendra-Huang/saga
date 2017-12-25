package saga

import "github.com/Shopify/sarama"

type Message struct {
	sarama.ConsumerMessage
}
