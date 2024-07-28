package data

import (
	"context"
	"database/sql"
	_ "database/sql"
	"fmt"
	"github.com/bunyawats/simple-go-htmx/data/mysql"
	"github.com/bunyawats/simple-go-htmx/model"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

type (
	Repository struct {
		q *mysql.Queries
		context.Context
	}
)

func NewRepository(mySqlDB *sql.DB, ctx context.Context) *Repository {
	queries := mysql.New(mySqlDB)

	return &Repository{
		q:       queries,
		Context: ctx,
	}
}

func (r *Repository) ListAllChunkPartition(taskId string) ([]string, error) {

	log.Println("call Repository.ListAllChunkPartition ", taskId)

	chunkPartitionList, err := r.q.ListAllChunkPartition(r.Context, taskId)
	if err != nil {
		log.Println(err)
		return chunkPartitionList, err
	}

	return chunkPartitionList, nil
}

func (r *Repository) CreateNewTask(t model.Task) error {

	log.Println("call Repository.CreateNewTask")

	task := mysql.CreateTaskParams{
		TaskID:       t.TaskID,
		TaskName:     t.TaskName,
		CronPattern:  t.CronPattern,
		InputFileUrl: t.InputFileUrl,
		TaskStatus:   sql.NullString{String: t.TaskStatus, Valid: true},
		ChunkSize:    sql.NullInt32{Int32: int32(t.ChunkSize), Valid: true},
	}

	log.Println("Create Task", task)
	_, err := r.q.CreateTask(r.Context, task)

	return err
}

func (r *Repository) CreateNewNotificationDetail(n model.NotificationDetail) error {

	log.Println("call Repository.CreateNewNotificationDetail")

	notificationDetail := mysql.CreateNotificationDetailParams{
		Email:          n.Email,
		ChunkPartition: n.ChunkPartition,
		TaskID:         n.TaskID,
	}

	log.Println("Create Notification Detail", notificationDetail)
	_, err := r.q.CreateNotificationDetail(r.Context, notificationDetail)

	return err
}

func (r *Repository) ListAllActiveTask() ([]model.Task, error) {

	log.Println("call Repository.ListAllActiveTask ")

	listActiveTasks := make([]model.Task, 0)
	taskList, err := r.q.ListAllActiveTasks(r.Context)
	if err != nil {
		fmt.Println(err)
		return listActiveTasks, err
	}
	for _, t := range taskList {
		task := model.Task{
			TaskID:      t.TaskID,
			TaskName:    t.TaskName,
			TaskStatus:  t.TaskStatus.String,
			CronPattern: t.CronPattern,
			ChunkSize:   int(t.ChunkSize.Int32),
		}
		listActiveTasks = append(listActiveTasks, task)
	}
	return listActiveTasks, nil
}

func (r *Repository) UpdateTaskStatus(taskId, status string) error {

	log.Println("call Repository.UpdateTaskStatus ")

	_, err := r.q.UpdateTaskStatus(r.Context, mysql.UpdateTaskStatusParams{
		TaskID:     taskId,
		TaskStatus: sql.NullString{String: status, Valid: true},
	})

	return err
}

func (r *Repository) GetTaskById(taskId string) (model.Task, error) {

	log.Printf("call Repository.GetTaskById: %v \n", taskId)

	t, err := r.q.GetTaskById(r.Context, taskId)
	if err != nil {
		return model.Task{}, err
	}

	task := model.Task{
		TaskID:      t.TaskID,
		TaskName:    t.TaskName,
		TaskStatus:  t.TaskStatus.String,
		CronPattern: t.CronPattern,
		ChunkSize:   int(t.ChunkSize.Int32),
	}

	return task, nil
}

func (r *Repository) ListNotiEmailByChunk(chunkId string) ([]string, error) {

	log.Printf("call Repository.ListNotiEmailByChunk: %v \n", chunkId)

	emailList, err := r.q.ListNotiEmailByChunk(r.Context, chunkId)
	if err != nil {
		log.Printf("Error reading email list: %v", err)
		return emailList, err
	}

	return emailList, nil
}
