package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"

	//"encoding/json"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/todo"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) (err error) {
	defer func() {
		if err != nil {
			err = todo.MustHandle(err)
		}
	}()
	var buff bytes.Buffer
	encoder := gob.NewEncoder(&buff)
	err = encoder.Encode(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/gob", Body: buff.Bytes()})
	if err != nil {
		return err
	}
	return nil
}
