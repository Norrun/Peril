package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func newPauseHandler(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}
func newMoveHandler(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(am)

		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			msg := gamelogic.RecognitionOfWar{
				Attacker: am.Player,
				Defender: gs.GetPlayerSnap(),
			}
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, fmt.Sprint(routing.WarRecognitionsPrefix, ".", gs.GetUsername()), msg)
			if err != nil {
				fmt.Println(err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}

	}
}

func newWarHandler(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(row gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(row)
		var result pubsub.AckType
		msg := ""
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			result = pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			result = pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon:
			msg = fmt.Sprintf("%s won a war against %s", winner, loser)
			result = pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			msg = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			result = pubsub.Ack
		default:
			fmt.Printf("error outcome was %d", outcome)
			result = pubsub.NackDiscard

		}
		err := publishGameLog(gs, ch, msg)
		if err != nil {
			fmt.Println(err)
			result = pubsub.NackRequeue
		}

		return result
	}
}

func publishGameLog(gs *gamelogic.GameState, ch *amqp.Channel, msg string) error {
	glog := routing.GameLog{CurrentTime: time.Now(), Message: msg, Username: gs.GetUsername()}
	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, fmt.Sprint(routing.GameLogSlug, ".", gs.GetUsername()), glog)
	if err != nil {
		return err
	}
	return nil
}
