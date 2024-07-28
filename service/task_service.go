package service

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/bunyawats/communication-management/data"
	"github.com/bunyawats/communication-management/model"
	"github.com/sony/sonyflake"
	"log"
	"os"
	"time"
)

const (
	dataInputFile = "data.csv"
	chunkSize     = 4
	mysqlUri      = "test:test@tcp(127.0.0.1:3306)/test?parseTime=true"
)

var (
	db  *sql.DB
	err error

	repo *data.Repository

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

}

func generateUniqProcessId() string {

	sf := sonyflake.NewSonyflake(sonyflake.Settings{})
	if sf == nil {
		log.Fatalf("Failed to initialize Sonyflake")
	}

	id, err := sf.NextID()
	if err != nil {
		log.Fatalf("Failed to generate ID: %v", err)
	}

	fmt.Printf("Generated Sonyflake ID: %d\n", id)
	return fmt.Sprintf("%v", id)
}

func CreatNewTask() (model.Task, error) {

	taskId := generateUniqProcessId()

	fmt.Println("Create New Task")

	file, err := os.Open(dataInputFile)
	if err != nil {
		log.Fatalf("failed to open file: %s", err)
	}
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("failed to read file: %s", err)
	}

	// Process the records
	chunkIndex := 0
	for _, record := range records {
		log.Printf("Email: %s\n", record[0])
		chunkPartition := fmt.Sprintf("%v_%v", taskId, chunkIndex)
		_ = repo.CreateNewNotificationDetail(model.NotificationDetail{
			Email:          record[0],
			ChunkPartition: chunkPartition,
			TaskID:         taskId,
		})

		if chunkIndex++; chunkIndex == chunkSize {
			chunkIndex = 0
		}
	}

	t := time.Now().Add(time.Minute)

	timeString := t.Format(model.TimeFormat)

	task := model.Task{
		TaskID:       taskId,
		TaskName:     fmt.Sprintf("%v %v", "send consent ont way", taskId),
		CronPattern:  timeString,
		InputFileUrl: dataInputFile,
		ChunkSize:    chunkSize,
		TaskStatus:   model.Status_Ceated,
	}
	err = repo.CreateNewTask(task)
	return task, err
}

func DeleteExistTask(taskId string) (model.Task, error) {

	task := model.Task{}
	err := repo.UpdateTaskStatus(taskId, model.Status_Inactive)
	if err != nil {
		log.Printf("can not delete exisit task %v", err)
	}
	task, err = repo.GetTaskById(taskId)
	if err != nil {
		log.Printf("can not get exisit task %v", err)
		return task, err
	}

	return task, nil
}

func ExecuteChunk(body []byte) {
	log.Printf("msg body: %s\n", body)
	chunkId := string(body)
	emailList, err := repo.ListNotiEmailByChunk(chunkId)
	if err != nil {
		log.Println(err)
		return
	}
	for _, email := range emailList {
		log.Printf("Notification Detail: %s\n", email)
	}

}
