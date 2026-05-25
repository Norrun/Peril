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
	defer todo.LogIgnoredErrorsOnPanic()
	onfatal := todo.GetResidualErrorsError

	fmt.Println("Starting Peril client...")
	con, err := amqp.Dial(pubsub.ConnectStr)
	if err != nil {
		log.Fatalln("Failed to connect(Dial) to amqp server: ", err)
	}
	defer nhelps.RunLogErr(con.Close, "error closing connection to amqp server")
	name, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalln(err, onfatal())
	}

	ch, err := con.Channel()
	if err != nil {
		log.Fatalln(err, onfatal())
	}
	//_, _, err = pubsub.DeclareAndBind(con, routing.ExchangePerilDirect, , routing.PauseKey, pubsub.Transient)
	state := gamelogic.NewGameState(name)

	err = Subscriptions(name, con, ch, state)
	if err != nil {
		log.Fatalln(err, onfatal())
	}

	gameLoop(state, ch, name)

	fmt.Println("Quitting the game")
}

func gameLoop(state *gamelogic.GameState, ch *amqp.Channel, name string) {
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
			am, err := state.CommandMove(input)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			err = pubsub.PublishJSON(ch,
				routing.ExchangePerilTopic, fmt.Sprint(routing.ArmyMovesPrefix, ".", name),
				am,
			)
			if err != nil {
				//fmt.Println(err.Error())
				continue
			}
			fmt.Println("move succesfull")
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
}

func Subscriptions(name string, con *amqp.Connection, ch *amqp.Channel, state *gamelogic.GameState) error {

	err := pubsub.SubscribeJSON(con,
		routing.ExchangePerilDirect,
		fmt.Sprint(routing.PauseKey, ".", name),
		routing.PauseKey, pubsub.Transient,
		newPauseHandler(state))
	if err != nil {
		return err
	}

	err = pubsub.SubscribeJSON(con,
		routing.ExchangePerilTopic,
		fmt.Sprint(routing.ArmyMovesPrefix, ".", name),
		fmt.Sprint(routing.ArmyMovesPrefix, ".", "*"),
		pubsub.Transient, newMoveHandler(state, ch),
	)
	if err != nil {
		return err
	}

	err = pubsub.SubscribeJSON(con,
		routing.ExchangePerilTopic, "war",
		fmt.Sprint(routing.WarRecognitionsPrefix, ".", "*"),
		pubsub.Durable, newWarHandler(state))
	if err != nil {
		return err
	}
	return nil
}
