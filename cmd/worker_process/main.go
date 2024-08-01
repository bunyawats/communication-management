package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/bunyawats/communication-management/data"
	"github.com/bunyawats/communication-management/service"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"log"
)

const (
	redisUri = "localhost:6379"
	mysqlUri = "test:test@tcp(127.0.0.1:3306)/test?parseTime=true"

	mutexName = "distributed-lock"
)

var (
	db  *sql.DB
	err error

	repo   *data.Repository
	client *redis.Client

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

	client = redis.NewClient(&redis.Options{
		Addr: redisUri,
	})
}

func main() {

	pool := goredis.NewPool(client)
	rs := redsync.New(pool)

	go service.ConsumeTasks(rs, executeTask)
	go service.ConsumeChunks()

	log.Println("waiting for messages")
	wait := make(chan struct{})
	<-wait
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
	}

	if ok, err := mutex.Unlock(); !ok || err != nil {
		log.Println("Could not release lock:", err)
	} else {
		log.Println("Task completed and lock released")
	}
}
