include .env

install:
	python3 -V \
	&& python3 -m venv venv \
	&& . venv/bin/activate \
	&& pip install --upgrade pip && pip install -r requirements.txt

check:
	black ./elt_pipeline --check

lint:
	flake8 ./elt_pipeline

test:
	docker exec elt_pipeline python -m pytest -vv --cov=utils tests/utils \
	&& docker exec elt_pipeline python -m pytest -vv --cov=ops tests/ops

pull:
	docker compose pull

build:
	docker compose build

build-dagster:
	docker build -t de_dagster:latest ./dockerimages/dagster

build-spark:
	docker build -t spark_master:latest ./dockerimages/spark

build-pipeline:
	docker build -t elt_pipeline:latest ./elt_pipeline

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

to_mysql:
	docker exec -it de_mysql mysql -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}

to_mysql_root:
	docker exec -it de_mysql mysql -u"root" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}

mysql_create:
	docker exec -it de_mysql mysql --local_infile -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE} -e"source /tmp/load_dataset/mysql_datasource.sql"

mysql_load:
	docker exec -it de_mysql mysql --local_infile -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE} -e"source /tmp/load_dataset/mysql_load.sql"

to_psql:
	docker exec -it de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}

to_psql_no_db:
	docker exec -it de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/postgres

psql_create:
	docker exec -it de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB} -f /tmp/load_dataset/psql_datasource.sql -a

dbt_install_deps:
	docker exec -it dbt_analytics cd /opt/dagster/app/dbt_analytics && dbt deps
