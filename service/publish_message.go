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

func publishMqMessage(msqBody string, qName string, ch *amqp091.Channel) {
	q, err := ch.QueueDeclare(
		qName,
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare queue to RabbitMQ")

	body := msqBody
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
	failOnError(err, "Failed to publish message to queue in RabbitMQ")
}

func SignalToAllSchedulerProcess(t model.Task) {

	conn, err := amqp091.Dial(model.RabbitUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer closeMqConnection(conn)
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer closeMqChannel(ch)

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

func EnqueueScanner(fileName string) {

	log.Printf("enqueueScanner: %v on schedule", fileName)
	conn, err := amqp091.Dial(model.RabbitUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer closeMqConnection(conn)
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer closeMqChannel(ch)

	publishMqMessage(fileName, "scanner_queue", ch)
}

func EnqueueTask(taskId string) {

	log.Printf("enqueueTask: %v on schedule", taskId)
	conn, err := amqp091.Dial(model.RabbitUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer closeMqConnection(conn)
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer closeMqChannel(ch)

	publishMqMessage(taskId, "task_queue", ch)
}

func EnqueueChunk(chunkList []string) {

	conn, err := amqp091.Dial(model.RabbitUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer closeMqConnection(conn)
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer closeMqChannel(ch)

	log.Println("Start enqueue chunks")
	for _, chunkPartition := range chunkList {
		log.Println(chunkPartition)
		publishMqMessage(chunkPartition, "chunk_queue", ch)
	}
}
