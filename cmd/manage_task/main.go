package main

import (
	"encoding/json"
	"flag"
	"github.com/bunyawats/simple-go-htmx/model"
	"github.com/bunyawats/simple-go-htmx/service"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

const (
	rabbitUri = "amqp://user:password@localhost:5672/"
)

func main() {

	cmd := flag.String("cmd", "no", "cmd: no add_task delete_task")
	taskId := flag.String("taskId", "", "cmd: delete_task taskId:12345...")

	// Parse the flags
	flag.Parse()

	task := model.Task{}
	if *cmd == "add_task" {
		task, _ = service.CreatNewTask()
	} else if *cmd == "delete_task" {
		if taskId == nil || *taskId == "" {
			log.Fatal("taskId is required")
		}
		task, _ = service.DeleteExistTask(*taskId)
	}
	signalToAllProcess(task)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func signalToAllProcess(t model.Task) {
	conn, err := amqp.Dial(rabbitUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {
			log.Fatalf("Failed to close RabbitMQ connection")
		}
	}(conn)

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {
			log.Fatalf("Failed to close RabbitMQ channel")
		}
	}(ch)

	err = ch.ExchangeDeclare(
		"topic_logs", // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	body, err := json.Marshal(t)
	if err != nil {
		log.Printf("Failed to marshal task %v", err)
	}

	err = ch.Publish(
		"topic_logs",     // exchange
		model.RoutingKey, // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}
