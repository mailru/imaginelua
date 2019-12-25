NAME=project
PORT=39031
ADMIN_PORT=39032
DOCKERFILE_PATH=example/Dockerfile

.PHONY: build clean run start stop list

build:
	cd .. && docker build -t $(NAME) -f $(DOCKERFILE_PATH) .

clean:
	docker rm -f $(NAME)
	docker rmi -f $(NAME)

run:
	docker run --restart=always -d -p $(PORT):3301 -p $(ADMIN_PORT):3302 --name $(NAME) $(NAME)
	docker port `docker ps -aq -f name=$(NAME)`

start:
	docker start $(NAME)

stop:
	docker stop `docker ps -aq -f name=$(NAME)`

list:
	docker images $(NAME)
	@echo
	docker ps -a -f name=$(NAME)
