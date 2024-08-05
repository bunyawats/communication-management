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

func publishMqMessage(msqBody string, qName string) {

	conn, err := amqp091.Dial(model.RabbitUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer closeMqConnection(conn)

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer closeMqChannel(ch)

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

func consumeMqMessage(qName string) (
	<-chan amqp091.Delivery,
	*amqp091.Connection,
	*amqp091.Channel,
) {

	conn, err := amqp091.Dial(model.RabbitUri)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

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
	return msgs, conn, ch
}

func ConsumeScanner() {

	msgs, conn, ch := consumeMqMessage("scanner_queue")
	defer closeMqConnection(conn)
	defer closeMqChannel(ch)

	for d := range msgs {
		log.Println("-----------------------------")
		log.Printf("executeScanner: %v\n", string(d.Body))
		go ExecuteScanner(Rs, d.Body)
	}
}

func ConsumeTasks() {

	msgs, conn, ch := consumeMqMessage("task_queue")
	defer closeMqConnection(conn)
	defer closeMqChannel(ch)

	for d := range msgs {
		log.Println("-----------------------------")
		log.Printf("executeTask: %v\n", string(d.Body))
		go ExecuteTask(Rs, d.Body)
	}
}

func ConsumeChunks() {

	msgs, conn, ch := consumeMqMessage("chunk_queue")
	defer closeMqConnection(conn)
	defer closeMqChannel(ch)

	for d := range msgs {
		ExecuteChunk(d.Body)
	}
}

func EnqueueScanner(fileName string) {

	log.Printf("enqueueScanner: %v on schedule", fileName)

	publishMqMessage(fileName, "scanner_queue")
}

func EnqueueTask(taskId string) {

	log.Printf("enqueueTask: %v on schedule", taskId)

	publishMqMessage(taskId, "task_queue")
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

		publishMqMessage(chunkPartition, "chunk_queue")
	}
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

func SubscribeSignal(removeJob func(taskId string),
	createJobForTask func(t model.Task),
) {

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
