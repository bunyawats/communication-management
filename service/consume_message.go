package service

import (
	"encoding/json"
	"github.com/bunyawats/communication-management/model"
	"github.com/rabbitmq/amqp091-go"
	"log"
)

func closeMqConnection(conn *amqp091.Connection) {
	err := conn.Close()
	if err != nil {
		log.Printf("Error closing RabbitMQ connection: %v", err)
	}
}

func closeMqChannel(ch *amqp091.Channel) {
	err := ch.Close()
	if err != nil {
		log.Printf("Error closing RabbitMQ connection: %v", err)
	}
}

func consumeMqMessage(qName string, ch *amqp091.Channel) <-chan amqp091.Delivery {
	q, err := ch.QueueDeclare(
		qName,
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare queue in RabbitMQ")

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to message from queue in RabbitMQ")
	return msgs
}

func SubscribeSignal(removeJob func(taskId string), createJobForTask func(t model.Task)) {

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

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,           // queue name
		model.RoutingKey, // routing key
		"topic_logs",     // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	task := model.Task{}
	for d := range msgs {
		err := json.Unmarshal(d.Body, &task)
		if err != nil {
			log.Printf("Error unmarshalling task: %v\n", err)
		}
		log.Printf(" [x] Received a message: %s", task)
		if task.TaskStatus == model.Status_Inactive {
			removeJob(task.TaskID)
		} else if task.TaskStatus == model.Status_Ceated {
			createJobForTask(task)
		}
	}

}

func ConsumeScanner() {

	conn, err := amqp091.Dial(model.RabbitUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer closeMqConnection(conn)
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer closeMqChannel(ch)

	msgs := consumeMqMessage("scanner_queue", ch)
	for d := range msgs {
		log.Println("-----------------------------")
		log.Printf("executeScanner: %v\n", string(d.Body))
		go ExecuteScanner(Rs, d.Body)
	}
}

func ConsumeTasks() {

	conn, err := amqp091.Dial(model.RabbitUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer closeMqConnection(conn)
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer closeMqChannel(ch)

	msgs := consumeMqMessage("task_queue", ch)
	for d := range msgs {
		log.Println("-----------------------------")
		log.Printf("executeTask: %v\n", string(d.Body))
		go ExecuteTask(Rs, d.Body)
	}
}

func ConsumeChunks() {

	conn, err := amqp091.Dial(model.RabbitUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer closeMqConnection(conn)
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer closeMqChannel(ch)

	msgs := consumeMqMessage("chunk_queue", ch)
	for d := range msgs {
		ExecuteChunk(d.Body)
	}
}
