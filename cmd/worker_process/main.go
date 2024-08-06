package main

import (
	"context"
	"database/sql"
	"github.com/bunyawats/communication-management/data"
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

	go mqService.ConsumeScanner(taskService.ExecuteScanner)
	go mqService.ConsumeTasks(taskService.ExecuteTask)
	go mqService.ConsumeChunks(taskService.ExecuteChunk)

	log.Println("waiting for messages")
	wait := make(chan struct{})
	<-wait
}
