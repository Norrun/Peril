package pubsub

import (
	"context"
	"encoding/json"

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

	subscribe(conn, exchange, queueName, key, queueType, handler, func(b []byte) (T, error) {
		var res T
		return res, json.Unmarshal(b, &res)
	})

	return nil
}
