# Umba Exam

Para levantar el ambiente

```bash
docker build . -t umba-airflow
```

Luego

```bash
docker-compose up -d
```

Se requiere agregar la coneccion `custom_s3` para la ejecucion del DAG

Dentro del contenedor de `./airflow.sh`

```bash
airflow connections add 'custom_s3' --conn-uri 's3://:minioadmin@?aws_access_key_id=minioadmin&aws_secret_access_key=minioadmin&host=http%3A%2F%2Fminio%3A9000'
```

### Para correr pruebas

#### Opcion 1)

Se puede abrir un bash

```bash
./airflow.sh bash
```

Y luego hacer el siguiente comando para comprobacion:

```bash
python dags/my-tutorial.py
```

O tests:

```bash
airflow dags test my-tutorial 2020-01-01
```

```sh
airflow tasks test my-tutorial download 2020-01-01
```

#### Opcion 2)

Lanzar pruebas inmediato

```bash
./airflow.sh airflow tasks test my-tutorial download 2020-01-01
```

O comprobacion inmediata

```bash
./airflow.sh python dags/my-tutorial.py
```
