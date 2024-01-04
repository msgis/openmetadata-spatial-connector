COMPOSE_FILE ?= docker/docker-compose.yml

run:
	docker compose -f $(COMPOSE_FILE) down -v && docker compose -f $(COMPOSE_FILE) up --build

