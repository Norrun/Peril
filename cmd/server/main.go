package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/nhelps"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	log.Println("Starting Peril server...")
	const conStr string = "amqp://guest:guest@localhost:5672/"
	con, err := amqp.Dial(conStr)
	if err != nil {
		log.Fatalln("Failed to connect(Dial) to amqp server: ", err)
	}
	defer nhelps.RunLogErr(con.Close, "error closing connection to amqp server")
	log.Println("Connection to broker succesfull")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
	log.Println("Server is shutting down")
	//nhelps.RunLogErr(con.Close, "failed to close connectiion on shutdown")
}
