package main

import (
	"context"
	"database/sql"
	"github.com/bunyawats/simple-go-htmx/service"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

const (
	rabbitUri = "amqp://user:password@localhost:5672/"
)

var (
	db  *sql.DB
	err error

	ctx = context.Background()
)

func main() {

	forever := make(chan struct{})

	go consumeChunks()

	log.Println("waiting for messages")
	// Block forever
	<-forever
}

func consumeChunks() {
	conn, err := amqp.Dial(rabbitUri)
	if err != nil {
		log.Fatal(err)
	}
	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {
			log.Printf("error closing amqp connection: %v", err)
		}
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {
			log.Printf("error closing amqp channel: %v", err)
		}
	}(ch)

	q, err := ch.QueueDeclare(
		"chunk_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	for d := range msgs {
		service.ExecuteChunk(d.Body)
	}
}
