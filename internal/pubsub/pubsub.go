package pubsub

import (
	"context"
	"encoding/json"
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

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	bod, err := json.Marshal(val)
	if err != nil {
		return err
	}
	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: bod})
	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) (err error) {
	/*defer func() {
		if err != nil {
			err = todo.MustHandle(err)
		}
	}()*/

	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	delivoryCh, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for delivory := range delivoryCh {
			var arg T
			err := json.Unmarshal(delivory.Body, &arg)
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
