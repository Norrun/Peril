package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/nhelps"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/todo"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	log.Println("Starting Peril server...")
	gamelogic.PrintServerHelp()
	defer log.Println(todo.GetResidualErrorsError())
	defer todo.LogIgnoredErrorsOnPanic()

	con, err := amqp.Dial(pubsub.ConnectStr)
	if err != nil {
		log.Fatalln("Failed to connect(Dial) to amqp server: ", err)
	}
	defer nhelps.RunLogErr(con.Close, "error closing connection to amqp server")

	log.Println("Connection to broker succesfull")

	err = pubsub.SubscribeGob(con,
		routing.ExchangePerilTopic, routing.GameLogSlug, fmt.Sprint(routing.GameLogSlug, ".", "*"), pubsub.Durable, func(entry routing.GameLog) pubsub.AckType {
			defer fmt.Print("> ")
			err := gamelogic.WriteLog(entry)
			if err != nil {
				log.Println("error on logging ", err)
				return pubsub.NackDiscard
			}
			return pubsub.Ack
		})
	if err != nil {
		log.Fatalln(err)
	}
	qChan, err := con.Channel()
	if err != nil {
		log.Fatalln(err)
	}
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		stop := false
		switch input[0] {
		case routing.PauseKey:
			fmt.Println("Pausing")
			pubsub.PublishJSON(qChan,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true})
		case "resume":
			fmt.Println("Resuming")
			pubsub.PublishJSON(qChan,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false})
		case "quit":
			fmt.Println("Quitting")
			stop = true
		default:
			fmt.Println("Unknown command")
		}
		if stop {
			break
		}

	}

	/*sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan*/
	log.Println("Server is shutting down")
	//nhelps.RunLogErr(con.Close, "failed to close connectiion on shutdown")
}
