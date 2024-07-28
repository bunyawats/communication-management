package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/bunyawats/simple-go-htmx/data"
	"github.com/bunyawats/simple-go-htmx/model"
	"github.com/go-redis/redis/v8"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sony/sonyflake"
	"log"
	"os"
	"time"
)

const (
	redisUri      = "localhost:6379"
	rabbitUri     = "amqp://user:password@localhost:5672/"
	mysqlUri      = "test:test@tcp(127.0.0.1:3306)/test?parseTime=true"
	dataInputFile = "data.csv"
	chunkSize     = 4
)

var (
	//	tasksProcessed = prometheus.NewCounter(prometheus.CounterOpts{
	//		Name: "tasks_processed_total",
	//		Help: "Total number of tasks processed",
	//	})

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

	repo = data.NewRepository(db, ctx)

	_ = redis.NewClient(&redis.Options{
		Addr:     redisUri,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

}

func main() {

	cmd := flag.String("cmd", "no", "cmd: no add_task delete_task")
	taskId := flag.String("taskId", "", "cmd: delete_task taskId:12345...")

	// Parse the flags
	flag.Parse()

	if *cmd == "add_task" {
		*taskId = generateUniqProcessId()
		creatNewTask(*taskId)
	} else if *cmd == "delete_task" {
		if taskId == nil || *taskId == "" {
			log.Fatal("taskId is required")
		}
		deleteExistTask(*taskId)
	}

}

func deleteExistTask(taskId string) {

	err = repo.UpdateTaskStatus(taskId, model.Status_Inactive)
	if err != nil {
		log.Printf("can not delete exisit task %v", err)
		return
	}
	task, err := repo.GetTaskById(taskId)
	if err != nil {
		log.Printf("can not get exisit task %v", err)
	}

	signalToAllProcess(task)
}

func generateUniqProcessId() string {

	sf := sonyflake.NewSonyflake(sonyflake.Settings{})
	if sf == nil {
		log.Fatalf("Failed to initialize Sonyflake")
	}

	id, err := sf.NextID()
	if err != nil {
		log.Fatalf("Failed to generate ID: %v", err)
	}

	fmt.Printf("Generated Sonyflake ID: %d\n", id)
	return fmt.Sprintf("%v", id)
}

func creatNewTask(taskId string) {
	fmt.Println("Create New Task")

	file, err := os.Open(dataInputFile)
	if err != nil {
		log.Fatalf("failed to open file: %s", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("failed to read file: %s", err)
	}

	// Process the records
	chunkIndex := 0
	for _, record := range records {
		log.Printf("Email: %s\n", record[0])
		chunkPartition := fmt.Sprintf("%v_%v", taskId, chunkIndex)
		_ = repo.CreateNewNotificationDetail(model.NotificationDetail{
			Email:          record[0],
			ChunkPartition: chunkPartition,
			TaskID:         taskId,
		})

		if chunkIndex++; chunkIndex == chunkSize {
			chunkIndex = 0
		}
	}

	t := time.Now().Add(time.Minute)

	timeString := t.Format(model.TimeFormat)

	task := model.Task{
		TaskID:       taskId,
		TaskName:     fmt.Sprintf("%v %v", "send consent ont way", taskId),
		CronPattern:  timeString,
		InputFileUrl: dataInputFile,
		ChunkSize:    chunkSize,
		TaskStatus:   model.Status_Ceated,
	}
	_ = repo.CreateNewTask(task)

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
