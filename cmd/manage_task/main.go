package main

import (
	"flag"
	"github.com/bunyawats/communication-management/model"
	"github.com/bunyawats/communication-management/service"
	"log"
)

func main() {

	cmd := flag.String("cmd", "no", "cmd: no add_task delete_task")
	taskId := flag.String("taskId", "", "cmd: delete_task taskId:12345...")

	// Parse the flags
	flag.Parse()

	task := model.Task{}
	if *cmd == "add_task" {
		task, _ = service.CreatNewTask()
	} else if *cmd == "delete_task" {
		if taskId == nil || *taskId == "" {
			log.Fatal("taskId is required")
		}
		task, _ = service.DeleteExistTask(*taskId)
	}
	service.SignalToAllProcess(task)
}
