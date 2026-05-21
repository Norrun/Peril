package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/nhelps"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	con, err := amqp.Dial(pubsub.ConnectStr)
	if err != nil {
		log.Fatalln("Failed to connect(Dial) to amqp server: ", err)
	}
	defer nhelps.RunLogErr(con.Close, "error closing connection to amqp server")
	name, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalln(err)
	}
	_, _, err = pubsub.DeclareAndBind(con, routing.ExchangePerilDirect, fmt.Sprint(routing.PauseKey, ".", name), routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatalln(err)
	}
	//defer nhelps.RunLogErr(ch.Close,"error closing channel")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
}
