package pubsub

import (
	"fmt"
	"log"

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

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	delivoryCh, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for delivory := range delivoryCh {

			arg, err := unmarshaller(delivory.Body)

			if err != nil {
				log.Println(err)
				continue
			}
			react := handler(arg)
			switch react {
			case Ack:
				err = delivory.Ack(false)
			case NackRequeue:
				err = delivory.Nack(false, true)
			case NackDiscard:
				err = delivory.Nack(false, false)
			}
			//fmt.Printf("handler did %v", react)
			if err != nil {
				log.Println(err)
			}

		}
	}()
	return nil
}
