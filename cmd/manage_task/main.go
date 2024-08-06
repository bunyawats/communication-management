package main

import (
	"context"
	"database/sql"
	"flag"
	"github.com/bunyawats/communication-management/data"
	"github.com/bunyawats/communication-management/model"
	"github.com/bunyawats/communication-management/service"
	"log"
)

const (
	mysqlUri  = "test:test@tcp(127.0.0.1:3306)/test?parseTime=true"
	RabbitUri = "amqp://user:password@localhost:5672/"
)

var (
	taskService *service.TaskService
	mqService   *service.MessageService
)

func init() {

	db, err := sql.Open("mysql", mysqlUri)
	if err != nil {
		//l.Error("Fail on connect to MySql")
		log.Fatal("can not open database")
	}
	ctx := context.Background()
	repo := data.NewRepository(db, ctx)

	mqService, err = service.NewMessageService(RabbitUri)
	if err != nil {
		log.Fatal(err)
	}

	taskService = service.NewTaskService(repo)
}

func main() {

	cmd := flag.String("cmd", "no", "cmd: no add_task delete_task")
	taskId := flag.String("taskId", "", "cmd: delete_task taskId:12345...")

	// Parse the flags
	flag.Parse()

	task := model.Task{}
	if *cmd == "add_task" {
		manifestFile := "manifest.json"
		task, _ = taskService.CreatNewTask(manifestFile)
	} else if *cmd == "delete_task" {
		if taskId == nil || *taskId == "" {
			log.Fatal("taskId is required")
		}
		task, _ = taskService.DeleteExistTask(*taskId)
	}
	mqService.SignalToAllSchedulerProcess(task)
}
