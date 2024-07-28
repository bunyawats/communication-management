
CREATE TABLE `notification_detail`
(
    `notification_id` bigint       NOT NULL AUTO_INCREMENT,
    `email`           varchar(100) NOT NULL,
    `chunk_partition` varchar(100) NOT NULL,
    `task_id`         varchar(50)  NOT NULL,
    PRIMARY KEY (`notification_id`)
);

CREATE TABLE `task`
(
    `task_id`        varchar(50)  NOT NULL,
    `task_name`      varchar(100) NOT NULL,
    `cron_pattern`   varchar(20)  NOT NULL,
    `input_file_url` varchar(100) NOT NULL,
    `task_status`    varchar(100) DEFAULT NULL,
    `chunk_size`     int          DEFAULT NULL,
    PRIMARY KEY (`task_id`)
);

# CREATE TABLE `task_job`
# (
#     `task_id` varchar(100) NOT NULL,
#     `job_id`  varchar(100) NOT NULL,
#     PRIMARY KEY (`task_id`, `job_id`)
# )