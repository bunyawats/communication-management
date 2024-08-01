package main

import (
	"github.com/bunyawats/communication-management/service"
	"log"
)

func main() {

	go service.ConsumeScanner()
	go service.ConsumeTasks()
	go service.ConsumeChunks()

	log.Println("waiting for messages")
	wait := make(chan struct{})
	<-wait
}
