-- Redis
-  docker network create some-network
-  docker run --network some-network --name some-redis -p 6379:6379 -d redis redis-server --save 60 1 --loglevel warning
-  docker run -it --network some-network --rm redis redis-cli -h some-redis
-  redis-cli: SET mykey "Hello\nWorld"
-  redis-cli: GET mykey "Hello\nWorld"

-- RabbitMQ
- docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 15672:15672 -e RABBITMQ_DEFAULT_USER=user -e RABBITMQ_DEFAULT_PASS=password rabbitmq:3-management