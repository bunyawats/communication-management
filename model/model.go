package model

import (
	"github.com/google/uuid"
)

const (
	Status_Ceated   = "CREATED"
	Status_Inactive = "INACTIVE"

	RoutingKey = "#"

	TimeFormat = "2006-01-02 15:04:05"
)

type (
	Task struct {
		TaskID       string `json:"taskID,omitempty"`
		TaskName     string `json:"taskName,omitempty"`
		CronPattern  string `json:"cronPattern,omitempty"`
		InputFileUrl string `json:"inputFileUrl,omitempty"`
		TaskStatus   string `json:"taskStatus,omitempty"`
		ChunkSize    int    `json:"chunkSize,omitempty"`
	}

	NotificationDetail struct {
		Email          string `json:"email,omitempty"`
		ChunkPartition string `json:"chunkPartition,omitempty"`
		TaskID         string `json:"taskID,omitempty"`
	}

	TaskJobs map[string]uuid.UUID
)