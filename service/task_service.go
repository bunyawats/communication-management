package service

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/bunyawats/communication-management/data"
	"github.com/bunyawats/communication-management/model"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"github.com/sony/sonyflake"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	mutexName = "distributed-lock"
	redisUri  = "localhost:6379"
)

type TaskService struct {
	*data.Repository
	*redsync.Redsync
}

func NewTaskService(
	repository *data.Repository) *TaskService {

	client := redis.NewClient(&redis.Options{
		Addr: redisUri,
	})
	pool := goredis.NewPool(client)
	rs := redsync.New(pool)

	return &TaskService{
		Repository: repository,
		Redsync:    rs,
	}
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

func loadManifest(manifestFile string) (model.Manifest, error) {

	// Open the JSON file
	file, err := os.Open(manifestFile)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return model.Manifest{}, err
	}
	defer closeFile(file)

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

func closeFile(file *os.File) {
	err := file.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// MoveFile moves a file from src to dst
func moveProcessedFile(fullFilePath string) error {

	log.Printf("moveProcessedFile: %v\n", fullFilePath)

	// Create the destination directory if it doesn't exist
	processedDir := filepath.Join(filepath.Dir(fullFilePath), "processed")
	log.Printf("processedDir: %v\n", processedDir)

	if err := os.MkdirAll(processedDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Open the source file
	sourceFile, err := os.Open(fullFilePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer closeFile(sourceFile)

	separator := "/"

	// Find the last occurrence of the separator
	lastIndex := strings.LastIndex(fullFilePath, separator)

	// Get the filename part of the file path
	var filename string
	if lastIndex != -1 {
		filename = fullFilePath[lastIndex+1:]
	} else {
		filename = fullFilePath
	}

	// Create the destination file
	destinationFilePath := filepath.Join(processedDir, filename)
	log.Printf("destinationFilePath : %v\n", destinationFilePath)
	destinationFile, err := os.Create(destinationFilePath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer closeFile(destinationFile)

	// Copy the contents from source to destination
	if _, err := io.Copy(destinationFile, sourceFile); err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	// Remove the original source file
	if err := os.Remove(fullFilePath); err != nil {
		return fmt.Errorf("failed to remove source file: %w", err)
	}

	return nil
}

func (s *TaskService) ExecuteTask(body []byte, enqueueChunk func([]string)) {

	taskId := string(body)
	taskLockName := fmt.Sprintf("%s_%s", mutexName, taskId)
	log.Printf("taskLockName: %v", taskLockName)
	mutex := s.NewMutex(taskLockName)
	if err := mutex.TryLock(); err != nil {
		log.Printf("Could not obtain lock: %v\n", err)
		return
	}

	log.Printf("Obtained lock, executing task: %s\n", taskId)

	chunkPartitionList, err := s.ListAllChunkPartition(taskId)
	if err != nil {
		log.Println(err)
	} else {
		enqueueChunk(chunkPartitionList)
	}

	if ok, err := mutex.Unlock(); !ok || err != nil {
		log.Println("Could not release lock:", err)
	} else {
		log.Println("Task completed and lock released")
	}
}

func (s *TaskService) ExecuteScanner(body []byte, signal func(task model.Task)) {

	manifestFileName := string(body)
	taskLockName := fmt.Sprintf("%s_%s", mutexName, manifestFileName)
	log.Printf("taskLockName: %v", taskLockName)
	mutex := s.NewMutex(taskLockName)
	if err := mutex.TryLock(); err != nil {
		log.Printf("Could not obtain lock: %v\n", err)
		return
	}

	log.Printf("Obtained lock, executing scanner: %s\n", manifestFileName)
	manifestFile := "../manage_task/manifest.json"
	task, err := s.CreatNewTask(manifestFile)
	if err != nil {
		log.Println(err)
		return
	}
	signal(task)

	if ok, err := mutex.Unlock(); !ok || err != nil {
		log.Println("Could not release lock:", err)
	} else {
		log.Println("Task completed and lock released")
	}
}

func (s *TaskService) CreatNewTask(manifestFile string) (model.Task, error) {

	taskId := generateUniqProcessId()
	log.Println("Create New Task")

	manifest, err := loadManifest(manifestFile)
	if err != nil {
		log.Printf("Error loading manifest: %v", err)
		return model.Task{}, err
	}
	log.Printf("Manifest: %v", manifest)

	dir := filepath.Dir(manifestFile)
	dataInputFile := dir + "/" + manifest.Data.DataFiles[0]
	log.Printf("dataInputFile: %v", dataInputFile)

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
		log.Printf("failed to read file: %s", err)
		return model.Task{}, err
	}

	// Process the records
	chunkIndex := 0
	chunkCounter := 0
	for _, record := range records {
		log.Printf("Email: %s\n", record[0])
		chunkPartition := fmt.Sprintf("%v_%v", taskId, chunkCounter)
		_ = s.CreateNewNotificationDetail(model.NotificationDetail{
			Email:          record[0],
			ChunkPartition: chunkPartition,
			TaskID:         taskId,
		})

		if chunkIndex++; chunkIndex == manifest.Data.ChunkSize {
			chunkIndex = 0
			chunkCounter++
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
	err = s.CreateNewTask(task)

	err = moveProcessedFile(manifestFile)
	err = moveProcessedFile(dataInputFile)

	return task, err
}

func (s *TaskService) DeleteExistTask(taskId string) (model.Task, error) {

	task := model.Task{}
	err := s.UpdateTaskStatus(taskId, model.Status_Inactive)
	if err != nil {
		log.Printf("can not delete exisit task %v", err)
	}
	task, err = s.GetTaskById(taskId)
	if err != nil {
		log.Printf("can not get exisit task %v", err)
		return task, err
	}

	return task, nil
}

func (s *TaskService) ExecuteChunk(body []byte) {
	log.Printf("msg body: %s\n", body)
	chunkId := string(body)
	emailList, err := s.ListNotiEmailByChunk(chunkId)
	if err != nil {
		log.Println(err)
		return
	}
	for _, email := range emailList {
		log.Printf("Notification Detail: %s\n", email)
	}

}
