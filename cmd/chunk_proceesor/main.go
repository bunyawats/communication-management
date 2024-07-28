package main

import (
	"context"
	"database/sql"
	"github.com/bunyawats/simple-go-htmx/data"
	"github.com/streadway/amqp"
	"log"
)

const (
	rabbitUri = "amqp://user:password@localhost:5672/"
	mysqlUri  = "test:test@tcp(127.0.0.1:3306)/test?parseTime=true"
)

var (
	db  *sql.DB
	err error

	repo *data.Repository

	ctx = context.Background()
)

func init() {
	//	prometheus.MustRegister(tasksProcessed)
	db, err = sql.Open("mysql", mysqlUri)
	if err != nil {
		//l.Error("Fail on connect to MySql")
		log.Fatal("can not open database")
	}
}

func main() {

	forever := make(chan struct{})

	repo = data.NewRepository(db, ctx)

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
		executeTask(d.Body)
	}
}

func executeTask(body []byte) {
	log.Printf("msg body: %s\n", body)
	chunkId := string(body)
	emailList, err := repo.ListNotiEmailByChunk(chunkId)
	if err != nil {
		log.Println(err)
		return
	}
	for _, email := range emailList {
		log.Printf("Notification Detail: %s\n", email)
	}

}
