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
	redisUri = "localhost:6379"
	mysqlUri = "test:test@tcp(127.0.0.1:3306)/test?parseTime=true"
)

var (
	db  *sql.DB
	err error

	repo *data.Repository

	ctx = context.Background()

	scheduler gocron.Scheduler

	taskJobs = make(model.TaskJobs)
)

func init() {

	db, err = sql.Open("mysql", mysqlUri)
	if err != nil {
		//l.Error("Fail on connect to MySql")
		log.Fatal("can not open database")
	}

	repo = data.NewRepository(db, ctx)

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
		log.Printf("active task %d, %v \n", i, t.TaskName)
		createJobForTask(t)
	}
	createManifestScannerTask()

	// start the scheduler
	scheduler.Start()
	go service.SubscribeSignal(removeJob, createJobForTask)

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
				service.EnqueueScanner(manifestFile)
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
