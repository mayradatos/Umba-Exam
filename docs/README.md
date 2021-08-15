# Umba Airflow

Enlaces Relevantes

- [Justificacion de Diseño](docs/DesignPrinciples.md)

## Quick Start

Pre-requisitos:

- Docker
- Docker Compose
- Git

Clona el repositorio y dentro del directorio ejecuta

```sh
docker build . -t umba-airflow
```

Seguido de

```sh
docker-compose up -d
```

Comprobamos que todo este levantado y la intefaz este displonible en `localhost:8080`

```sh
$ docker ps
CONTAINER ID   IMAGE          COMMAND                  CREATED      STATUS                 PORTS                                                           NAMES
d415fbd3356a   umba-airflow   "/usr/bin/dumb-init …"   4 days ago   Up 8 hours (healthy)   8080/tcp                                                        airflow_airflow-worker_1
02f61ffd4807   umba-airflow   "/usr/bin/dumb-init …"   4 days ago   Up 8 hours (healthy)   8080/tcp                                                        airflow_airflow-scheduler_1
3936d4a903d1   umba-airflow   "/usr/bin/dumb-init …"   4 days ago   Up 8 hours (healthy)   0.0.0.0:8080->8080/tcp, :::8080->8080/tcp                       airflow_airflow-webserver_1
ad8a47d99fbc   umba-airflow   "/usr/bin/dumb-init …"   4 days ago   Up 8 hours (healthy)   0.0.0.0:5555->5555/tcp, :::5555->5555/tcp, 8080/tcp             airflow_flower_1
bda4bbed7118   minio/minio    "/usr/bin/docker-ent…"   4 days ago   Up 8 hours             0.0.0.0:9000-9001->9000-9001/tcp, :::9000-9001->9000-9001/tcp   airflow_minio_1
8ebae0e869e5   postgres:13    "docker-entrypoint.s…"   4 days ago   Up 8 hours (healthy)   5432/tcp                                                        airflow_postgres_1
e1cfa7c93c80   redis:latest   "docker-entrypoint.s…"   4 days ago   Up 8 hours (healthy)   0.0.0.0:6379->6379/tcp, :::6379->6379/tcp                       airflow_redis_1
```

Finalmente se requiere agregar la coneccion `custom_s3` para la ejecucion del DAG, levantamos un worker node:

Para Unix (Mac/Linux)

```sh
./airflow.sh bash
```

Para Windows:

```sh
docker-compose run airflow-worker bash
```

Una vez estando dentro del contenedor:

```sh
airflow connections add 'custom_s3' --conn-uri 's3://:minioadmin@?aws_access_key_id=minioadmin&aws_secret_access_key=minioadmin&host=http%3A%2F%2Fminio%3A9000'
```

### Pruebas

#### Parse Test

Dentro del contenedor del worker:

```sh
python dags/umbaDag.py
```

#### Full Dag Test

```sh
airflow dags test UmbaExam 2020-01-01
```

#### Single Task Test

```sh
airflow tasks test UmbaExam [TaskName] 2020-01-01
```
