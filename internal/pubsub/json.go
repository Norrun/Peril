package pubsub

import (
	"context"
	"encoding/json"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/todo"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) (err error) {
	defer func() {
		if err != nil {
			err = todo.MustHandle(err)
		}
	}()
	bod, err := json.Marshal(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: bod})
	if err != nil {
		return err
	}
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
	defer func() {
		if err != nil {
			err = todo.MustHandle(err)
		}
	}()

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
