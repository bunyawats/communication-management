
/* name: ListAllChunkPartition :many */
SELECT chunk_partition
FROM notification_detail
WHERE task_id = ?
GROUP BY chunk_partition;

/* name: ListNotiEmailByChunk :many */
SELECT email
FROM notification_detail
WHERE chunk_partition = ?;

/* name: ListAllActiveTasks :many */
SELECT task_id, task_name, cron_pattern, input_file_url, task_status, chunk_size
FROM task
WHERE task_status != "INACTIVE";

/* name: CreateTask :execresult */
INSERT INTO task
(task_id, task_name, cron_pattern, input_file_url, task_status, chunk_size)
VALUES (?, ?, ?, ?, ?, ?);

/* name: CreateNotificationDetail :execresult */
INSERT INTO notification_detail
    (email, chunk_partition, task_id)
VALUES (?, ?, ?);

/* name: UpdateTaskStatus :execresult */
UPDATE task
SET task_status=?
WHERE task_id = ?;

/* name: GetTaskById :one */
SELECT task_id, task_name, cron_pattern, input_file_url, task_status, chunk_size
FROM task
WHERE task_id = ?;