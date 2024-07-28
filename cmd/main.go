package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/bunyawats/simple-go-htmx/data"
	"github.com/bunyawats/simple-go-htmx/model"
	"github.com/go-co-op/gocron/v2"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"log"
	"time"

	//"github.com/prometheus/client_golang/prometheus"
	//"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	redisUri  = "localhost:6379"
	rabbitUri = "amqp://user:password@localhost:5672/"
	mysqlUri  = "test:test@tcp(127.0.0.1:3306)/test?parseTime=true"
	mutexname = "distributed-lock"
)

var (
	//	tasksProcessed = prometheus.NewCounter(prometheus.CounterOpts{
	//		Name: "tasks_processed_total",
	//		Help: "Total number of tasks processed",
	//	})

	db  *sql.DB
	err error

	repo   *data.Repository
	client *redis.Client

	ctx = context.Background()

	scheduler gocron.Scheduler

	taskJobs = make(model.TaskJobs)
)

func init() {
	//	prometheus.MustRegister(tasksProcessed)
	db, err = sql.Open("mysql", mysqlUri)
	if err != nil {
		//l.Error("Fail on connect to MySql")
		log.Fatal("can not open database")
	}

	repo = data.NewRepository(db, ctx)

	client = redis.NewClient(&redis.Options{
		Addr: redisUri,
	})

	scheduler, err = gocron.NewScheduler()
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	pool := goredis.NewPool(client)
	rs := redsync.New(pool)

	allTasks, err := repo.ListAllActiveTask()
	if err != nil {
		log.Fatal(err)
	}
	for i, t := range allTasks {
		log.Printf("active task %d, %v \n", i, t.TaskName)
		createJobForTask(t)
	}

	go consumeTasks(rs)
	//go func() {
	//	http.Handle("/metrics", promhttp.Handler())
	//	log.Fatal(http.ListenAndServe(":2112", nil))
	//}()
	go subscribeSignal()

	// start the scheduler
	scheduler.Start()

	wait := make(chan struct{})
	<-wait

	// block until you are ready to shut down
	//select {
	//case <-time.After(time.Minute):
	//}

	// when you're done, shut it down
	//err = s.Shutdown()
	//if err != nil {
	//	log.Fatal(err)
	//}
}

func createJobForTask(t model.Task) {

	jobDefinition := createJobDefinition(t.CronPattern)
	// add a job to the scheduler
	j, err := scheduler.NewJob(
		jobDefinition,
		gocron.NewTask(
			func(t model.Task) {
				log.Printf("enqueueTask: %v on schedule", t.TaskID)
				enqueueTask(t)
			},
			t,
		),
	)
	if err != nil {
		log.Printf("Error creating job: %v", err)
		return
	}
	// each job has a unique id
	log.Printf("jobID: %v\n", j.ID())
	taskJobs[t.TaskID] = j.ID()
}

func createJobDefinition(pattern string) gocron.JobDefinition {

	bangkokLocation, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		log.Fatalf("Failed to load location: %v", err)
	}
	t, err := time.ParseInLocation(model.TimeFormat, pattern, bangkokLocation)
	if err != nil {
		log.Printf("Failed to parse time: %v", err)
		return gocron.CronJob(pattern, false)
	}

	log.Printf("time: %v\n", t)
	return gocron.OneTimeJob(
		gocron.OneTimeJobStartDateTime(t),
	)

}

func enqueueTask(task model.Task) {

	//	log.Printf("enqueueTask: %v\n", task.TaskID)
	conn, err := amqp.Dial(rabbitUri)
	if err != nil {
		log.Print(err)
	}
	defer func(conn *amqp.Connection) {
		_ = conn.Close()
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer func(ch *amqp.Channel) {
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
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	if err != nil {
		log.Fatal(err)
	}
	//	log.Println("Enqueued task")
}

func consumeTasks(rs *redsync.Redsync) {
	conn, err := amqp.Dial(rabbitUri)
	if err != nil {
		log.Fatal(err)
	}
	defer func(conn *amqp.Connection) {
		_ = conn.Close()
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer func(ch *amqp.Channel) {
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
		go executeTask(rs, d.Body)
	}
}

func executeTask(rs *redsync.Redsync, body []byte) {

	taskId := string(body)
	taskLockName := fmt.Sprintf("%s_%s", mutexname, taskId)
	log.Printf("taskLockName: %v", taskLockName)
	mutex := rs.NewMutex(taskLockName)
	if err := mutex.TryLock(); err != nil {
		log.Printf("Could not obtain lock: %v\n", err)
		return
	}

	log.Printf("Obtained lock, executing task: %s\n", taskId)

	chunkPartitionList, err := repo.ListAllChunkPartition(taskId)
	if err != nil {
		log.Println(err)
	} else {
		enqueueChunk(chunkPartitionList)
		//tasksProcessed.Inc()
	}

	// Add delay to make sure redis can obtain the mutex lock.
	//time.Sleep(5 * time.Second)

	if ok, err := mutex.Unlock(); !ok || err != nil {
		log.Println("Could not release lock:", err)
	} else {
		log.Println("Task completed and lock released")
	}
}

func enqueueChunk(chunkList []string) {

	conn, err := amqp.Dial(rabbitUri)
	if err != nil {
		log.Print(err)
	}
	defer func(conn *amqp.Connection) {
		_ = conn.Close()
	}(conn)

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer func(ch *amqp.Channel) {
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
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(body),
			})
		if err != nil {
			log.Fatal(err)
		}

	}

	log.Println("Enqueued chunks")
}

func subscribeSignal() {

	conn, err := amqp.Dial(rabbitUri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer func(conn *amqp.Connection) {
		err := conn.Close()
		if err != nil {
			log.Printf("Error closing RabbitMQ connection: %v", err)
		}
	}(conn)

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer func(ch *amqp.Channel) {
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

func removeJob(taskId string) {
	log.Printf("inactive taskId: %v", taskId)

	jobId := taskJobs[taskId]
	err = scheduler.RemoveJob(jobId)
	if err != nil {
		log.Printf("Error removing job: %v\n", err)
	}

}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
