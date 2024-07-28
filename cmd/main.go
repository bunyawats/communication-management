package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/bunyawats/communication-management/data"
	"github.com/bunyawats/communication-management/model"
	"github.com/bunyawats/communication-management/service"
	"github.com/go-co-op/gocron/v2"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"log"
	"time"
)

const (
	redisUri = "localhost:6379"
	mysqlUri = "test:test@tcp(127.0.0.1:3306)/test?parseTime=true"

	mutexName = "distributed-lock"
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

	go service.ConsumeTasks(rs, executeTask)
	go service.SubscribeSignal(removeJob, createJobForTask)

	//go func() {
	//	http.Handle("/metrics", promhttp.Handler())
	//	log.Fatal(http.ListenAndServe(":2112", nil))
	//}()

	// start the scheduler
	scheduler.Start()

	wait := make(chan struct{})
	<-wait

	// when you're done, shut it down
	//err = s.Shutdown()
	//if err != nil {
	//	log.Fatal(err)
	//}
}

func executeTask(rs *redsync.Redsync, body []byte) {

	taskId := string(body)
	taskLockName := fmt.Sprintf("%s_%s", mutexName, taskId)
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
		service.EnqueueChunk(chunkPartitionList)
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

func createJobForTask(t model.Task) {

	jobDefinition := createJobDefinition(t.CronPattern)
	// add a job to the scheduler
	j, err := scheduler.NewJob(
		jobDefinition,
		gocron.NewTask(
			func(t model.Task) {
				log.Printf("enqueueTask: %v on schedule", t.TaskID)
				service.EnqueueTask(t)
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

func removeJob(taskId string) {
	log.Printf("inactive taskId: %v", taskId)

	jobId := taskJobs[taskId]
	err = scheduler.RemoveJob(jobId)
	if err != nil {
		log.Printf("Error removing job: %v\n", err)
	}

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
