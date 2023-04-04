include .env

build:
	docker compose build

build-dagster:
	docker build -t de_dagster:latest ./dockerimages/dagster

build-spark:
	docker build -t spark_master:latest ./dockerimages/spark

up-bg:
	docker compose --env-file .env up -d

up:
	docker compose --env-file .env up

down:
	docker compose --env-file .env down

restart-bg:
	docker compose --env-file .env down && docker compose --env-file .env up -d

restart:
	docker compose --env-file .env down && docker compose --env-file .env up
