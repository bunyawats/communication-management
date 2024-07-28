package main

import (
	"github.com/bunyawats/communication-management/service"
	"log"
)

func main() {

	forever := make(chan struct{})

	go service.ConsumeChunks()

	log.Println("waiting for messages")
	// Block forever
	<-forever
}
