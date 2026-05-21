package main

import (
	"fmt"
	"log"

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
	//_, _, err = pubsub.DeclareAndBind(con, routing.ExchangePerilDirect, , routing.PauseKey, pubsub.Transient)
	state := gamelogic.NewGameState(name)
	err = pubsub.SubscribeJSON(con, routing.ExchangePerilDirect, fmt.Sprint(routing.PauseKey, ".", name), routing.PauseKey, pubsub.Transient, handlerPause(state))
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
		case "spawn":
			err := state.CommandSpawn(input)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
		case "move":
			_, err := state.CommandMove(input)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			//fmt.Printf("moved %v to %v successfully", movement.Units, movement.ToLocation)
		case "status":
			state.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			stop = true
		default:
			fmt.Println("unknown command")
		}
		if stop {
			break
		}

	}
	//defer nhelps.RunLogErr(ch.Close,"error closing channel")
	/*sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan*/
	fmt.Println("Quitting the game")
}
