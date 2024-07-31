package service

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/bunyawats/communication-management/data"
	"github.com/bunyawats/communication-management/model"
	"github.com/sony/sonyflake"
	"io"
	"log"
	"os"
	"time"
)

const (
	mysqlUri = "test:test@tcp(127.0.0.1:3306)/test?parseTime=true"
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

func loadManifest() (model.Manifest, error) {
	// Specify the file path
	filePath := "manifest.json"

	// Open the JSON file
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return model.Manifest{}, err
	}
	defer file.Close()

	// Read the file content
	byteValue, err := io.ReadAll(file)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return model.Manifest{}, err
	}

	// Unmarshal the JSON content into a map
	var manifest model.Manifest
	err = json.Unmarshal(byteValue, &manifest)
	if err != nil {
		fmt.Println("Error unmarshaling JSON:", err)
		return model.Manifest{}, err
	}

	return manifest, nil
}

func CreatNewTask() (model.Task, error) {

	taskId := generateUniqProcessId()

	log.Println("Create New Task")

	manifest, err := loadManifest()
	if err != nil {
		log.Fatalf("Error loading manifest: %v", err)
	}
	log.Printf("Manifest: %v", manifest)

	dataInputFile := manifest.Data.DataFiles[0]
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

		if chunkIndex++; chunkIndex == manifest.Data.ChunkSize {
			chunkIndex = 0
		}
	}

	t := time.Now().Add(time.Minute)
	timeString := t.Format(model.TimeFormat)
	log.Printf("time: %v", timeString)

	task := model.Task{
		TaskID:          taskId,
		TaskName:        manifest.Name,
		SchedulePattern: manifest.Data.SchedulePattern,
		InputFileUrl:    dataInputFile,
		ChunkSize:       manifest.Data.ChunkSize,
		TaskStatus:      model.Status_Ceated,
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
