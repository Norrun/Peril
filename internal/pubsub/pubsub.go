package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const ConnectStr string = "amqp://guest:guest@localhost:5672/"

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func (receiver AckType) String() string {
	switch receiver {
	case Ack:
		return "Ack"
	case NackRequeue:
		return "NackRequeue"
	case NackDiscard:
		return "NackDiscard"
	}
	return "Invalid AckType"
}

type SimpleQueueType int

const (
	Durable = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to Create Channel: %v", err)
	}
	queue, err := ch.QueueDeclare(queueName, queueType == Durable, queueType == Transient, queueType == Transient, false, amqp.Table{"x-dead-letter-exchange": "peril_dlx"})
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to Declear Queue: %v", err)
	}

	err = ch.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to Bind Queue: %v", err)
	}
	return ch, queue, nil
}
