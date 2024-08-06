package service

import (
	"encoding/json"
	"github.com/bunyawats/communication-management/model"
	"github.com/rabbitmq/amqp091-go"
	"log"
)

const (
	scannerQueueName = "scanner_queue"
	taskQueueNam     = "task_queue"
	chunkQueueName   = "chunk_queue"
	jobTopicName     = "job_topic"
)

type MessageService struct {
	*amqp091.Connection
}

func NewMessageService(rabbitUri string) (*MessageService, error) {

	conn, err := amqp091.Dial(rabbitUri)
	if err != nil {
		log.Printf("%s: %s", "Failed to connect to RabbitMQ", err)
		return nil, err
	}
	return &MessageService{Connection: conn}, nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func closeMqChannel(ch *amqp091.Channel) {
	err := ch.Close()
	if err != nil {
		log.Printf("Error closing RabbitMQ connection: %v", err)
	}
}

func (s MessageService) publishMqMessage(msqBody string, qName string) {

	ch, err := s.Channel()
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

func (s MessageService) consumeMqMessage(qName string) (
	<-chan amqp091.Delivery,
	*amqp091.Channel,
) {

	ch, err := s.Channel()
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
	return msgs, ch
}

func (s MessageService) ConsumeScanner(executor func([]byte, func(t model.Task))) {

	msgs, ch := s.consumeMqMessage(scannerQueueName)
	defer closeMqChannel(ch)

	for d := range msgs {
		log.Println("-----------------------------")
		log.Printf("executeScanner: %v\n", string(d.Body))
		go executor(d.Body, s.SignalToAllSchedulerProcess)
	}
}

func (s MessageService) ConsumeTasks(executor func([]byte, func([]string))) {

	msgs, ch := s.consumeMqMessage(taskQueueNam)
	defer closeMqChannel(ch)

	for d := range msgs {
		log.Println("-----------------------------")
		log.Printf("executeTask: %v\n", string(d.Body))
		go executor(d.Body, s.EnqueueChunk)
	}
}

func (s MessageService) ConsumeChunks(executor func(body []byte)) {

	msgs, ch := s.consumeMqMessage("chunk_queue")
	defer closeMqChannel(ch)

	for d := range msgs {
		executor(d.Body)
	}
}

func (s MessageService) EnqueueScanner(fileName string) {

	log.Printf("enqueueScanner: %v on schedule", fileName)

	s.publishMqMessage(fileName, scannerQueueName)
}

func (s MessageService) EnqueueTask(taskId string) {

	log.Printf("enqueueTask: %v on schedule", taskId)

	s.publishMqMessage(taskId, taskQueueNam)
}

func (s MessageService) EnqueueChunk(chunkList []string) {

	log.Println("Start enqueue chunks")
	for _, chunkPartition := range chunkList {
		log.Println(chunkPartition)

		s.publishMqMessage(chunkPartition, chunkQueueName)
	}
}

func (s MessageService) SignalToAllSchedulerProcess(t model.Task) {

	ch, err := s.Channel()
	failOnError(err, "Failed to open a channel")
	defer closeMqChannel(ch)

	err = ch.ExchangeDeclare(
		jobTopicName, // name
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
		jobTopicName,     // exchange
		model.RoutingKey, // routing key
		false,            // mandatory
		false,            // immediate
		amqp091.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}

func (s MessageService) SubscribeSignal(removeJob func(taskId string),
	createJobForTask func(t model.Task),
) {

	ch, err := s.Channel()
	failOnError(err, "Failed to open a channel")
	defer closeMqChannel(ch)

	err = ch.ExchangeDeclare(
		jobTopicName, // name
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
		jobTopicName,     // exchange
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
