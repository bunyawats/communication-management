package service

import (
	"encoding/json"
	"github.com/bunyawats/communication-management/model"
	"github.com/rabbitmq/amqp091-go"
	"log"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func SignalToAllSchedulerProcess(t model.Task) {
	conn, err := amqp091.Dial(model.RabbitUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer func(conn *amqp091.Connection) {
		err := conn.Close()
		if err != nil {
			log.Fatalf("Failed to close RabbitMQ connection")
		}
	}(conn)

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer func(ch *amqp091.Channel) {
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
		amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}

func EnqueueTask(task model.Task) {

	log.Printf("enqueueTask: %v on schedule", task.TaskID)

	conn, err := amqp091.Dial(model.RabbitUri)
	if err != nil {
		log.Print(err)
	}
	defer func(conn *amqp091.Connection) {
		_ = conn.Close()
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer func(ch *amqp091.Channel) {
		_ = ch.Close()
	}(ch)

	q, err := ch.QueueDeclare(
		"task_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	body := task.TaskID
	err = ch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp091.Publishing{
			DeliveryMode: amqp091.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	if err != nil {
		log.Fatal(err)
	}
}

func EnqueueChunk(chunkList []string) {

	conn, err := amqp091.Dial(model.RabbitUri)
	if err != nil {
		log.Print(err)
	}
	defer func(conn *amqp091.Connection) {
		_ = conn.Close()
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer func(ch *amqp091.Channel) {
		_ = ch.Close()
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

	log.Println("Start enqueue chunks")
	for _, chunkPartition := range chunkList {
		log.Println(chunkPartition)
		body := chunkPartition
		err = ch.Publish(
			"",
			q.Name,
			false,
			false,
			amqp091.Publishing{
				DeliveryMode: amqp091.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(body),
			})
		if err != nil {
			log.Fatal(err)
		}

	}

	log.Println("Enqueued chunks")
}
