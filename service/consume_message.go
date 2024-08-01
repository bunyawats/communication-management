package service

import (
	"encoding/json"
	"github.com/bunyawats/communication-management/model"
	"github.com/rabbitmq/amqp091-go"
	"log"
)

func ConsumeChunks() {
	conn, err := amqp091.Dial(model.RabbitUri)
	if err != nil {
		log.Fatal(err)
	}
	defer func(conn *amqp091.Connection) {
		err := conn.Close()
		if err != nil {
			log.Printf("error closing amqp connection: %v", err)
		}
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer func(ch *amqp091.Channel) {
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
		ExecuteChunk(d.Body)
	}
}

func ConsumeTasks() {
	conn, err := amqp091.Dial(model.RabbitUri)
	if err != nil {
		log.Fatal(err)
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
		log.Println("-----------------------------")
		log.Printf("executeTask: %v\n", string(d.Body))
		go ExecuteTask(Rs, d.Body)
	}
}

func SubscribeSignal(removeJob func(taskId string), createJobForTask func(t model.Task)) {

	conn, err := amqp091.Dial(model.RabbitUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer func(conn *amqp091.Connection) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing RabbitMQ connection: %v", err)
		}
	}(conn)

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer func(ch *amqp091.Channel) {
		err := ch.Close()
		if err != nil {
			log.Printf("Error closing RabbitMQ connection: %v", err)
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
