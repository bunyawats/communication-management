package main

import (
	"context"
	"database/sql"
	"github.com/bunyawats/communication-management/data"
	"github.com/bunyawats/communication-management/model"
	"github.com/bunyawats/communication-management/service"
	"github.com/go-co-op/gocron/v2"
	"log"
	"time"
)

const (
	mysqlUri  = "test:test@tcp(127.0.0.1:3306)/test?parseTime=true"
	RabbitUri = "amqp://user:password@localhost:5672/"

	TimeFormat = "2006-01-02 15:04:05"
)

var (
	repo      *data.Repository
	scheduler gocron.Scheduler
	taskJobs  = make(model.TaskJobs)
	mqService *service.MessageService
)

func init() {

	db, err := sql.Open("mysql", mysqlUri)
	if err != nil {
		//l.Error("Fail on connect to MySql")
		log.Fatal("can not open database")
	}
	ctx := context.Background()
	repo = data.NewRepository(db, ctx)

	mqService, err = service.NewMessageService(RabbitUri)
	if err != nil {
		log.Fatal(err)
	}

	scheduler, err = gocron.NewScheduler()
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	allTasks, err := repo.ListAllActiveTask()
	if err != nil {
		log.Fatal(err)
	}
	for i, t := range allTasks {
		log.Printf("active task %d, %v \n", i, t.TaskID)
		createJobForTask(t)
	}
	createManifestScannerTask()

	// start the scheduler
	scheduler.Start()
	go mqService.SubscribeSignal(removeJob, createJobForTask)

	log.Println("waiting for messages")
	wait := make(chan struct{})
	<-wait
}

func createManifestScannerTask() {
	//const scanCronPattern = "*/2 * * * *"
	const scanCronPattern = "* * * * *"
	const manifestFileName = "manifest.json"

	jobDefinition := gocron.CronJob(scanCronPattern, false)
	// add a job to the scheduler
	j, err := scheduler.NewJob(
		jobDefinition,
		gocron.NewTask(
			func(manifestFile string) {
				mqService.EnqueueScanner(manifestFile)
			},
			manifestFileName,
		),
	)
	if err != nil {
		log.Printf("Error creating job: %v", err)
		return
	}
	// each job has a unique id
	log.Printf("jobID: %v\n", j.ID())
}

func createJobForTask(t model.Task) {

	jobDefinition := createJobDefinition(t.SchedulePattern)
	// add a job to the scheduler
	j, err := scheduler.NewJob(
		jobDefinition,
		gocron.NewTask(
			func(t model.Task) {
				mqService.EnqueueTask(t.TaskID)
			},
			t,
		),
	)
	if err != nil {
		log.Printf("Error creating job: %v", err)
		return
	}
	// each job has a unique id
	log.Printf("taskId: %v - jobID: %v\n", t.TaskID, j.ID())
	taskJobs[t.TaskID] = j.ID()
}

func removeJob(taskId string) {
	log.Printf("inactive taskId: %v", taskId)

	jobId := taskJobs[taskId]
	log.Printf("remove jobId: %v", jobId)
	err := scheduler.RemoveJob(jobId)
	if err != nil {
		log.Printf("Error removing job: %v\n", err)
	}

}

func createJobDefinition(pattern string) gocron.JobDefinition {

	bangkokLocation, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		log.Fatalf("Failed to load location: %v", err)
	}
	t, err := time.ParseInLocation(TimeFormat, pattern, bangkokLocation)
	if err != nil {
		log.Printf("Failed to parse time: %v", err)
		return gocron.CronJob(pattern, false)
	}

	log.Printf("time: %v\n", t)
	return gocron.OneTimeJob(
		gocron.OneTimeJobStartDateTime(t),
	)

}
