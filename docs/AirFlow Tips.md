## Docker Compose

https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml

Agregar minio

```yaml
  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    ports:
      - 9000:9000
      - 9001:9001
    volumes:
      - ./S3:/data
```



## Como agregar dependencias de PIP

Se realiza en 3 pasos.

1. Extender imagen desde apache/airflow
2. Agregar nombre de la nueva imagen a `.env`
3. Recrear compose

#### Paso 1

Crea nuevo archivo llamado `Dockerfile` en la misma ruta que `docker-compose.yaml`

```dockerfile
FROM apache/airflow:latest
RUN pip install --no-cache-dir seaborn==0.11.1
```

Ejecutar comando `build` con tag personalizado:

```bash
docker build . -t umba-airflow
```

#### Paso 2

Al archivo `env` agregar la siguiente linea:

```bash
...
AIRFLOW_IMAGE_NAME=umba-airflow
...
```

#### Paso 3

Ejecutar el comando

```sh
docker-compose up -d
```

Para recrear los contenedores. Listo 👍

## Custom S3 repo

Conn id: `custom_s3` (Puede ser cualquier cosa)

Conn type: `S3`

`Extra`:

```json
{
  "aws_access_key_id": "minioadmin",
  "aws_secret_access_key": "minioadmin",
  "host": "http://minio:9000"
}
```

El resto deberan estar vacios.

## TaskFlow

https://airflow.apache.org/docs/apache-airflow/stable/concepts/taskflow.html

## Hook S3

https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/hooks/s3/index.html#module-airflow.providers.amazon.aws.hooks.s3

