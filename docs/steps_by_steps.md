# 1. Main scripts syntax
#### Kafka producer:
Start Producer: ```sudo docker start capstone-kafka-producer```

Logs: ```sudo docker logs -f capstone-kafka-producer```

Stop: ```docker stop capstone-kafka-producer```

#### Kafka consumer:
Start Consumer: ```sudo docker start capstone-kafka-consumer```

Logs: ```sudo docker logs -f capstone-kafka-consumer```

Stop: ```docker stop capstone-kafka-consumer```

#### Streamlit Chatbot:
Streamlit: ```uv run --env-file .env streamlit run scripts/chatbot/chatbot_app.py```

#### dbt docs serve:
dbt docs serve: ```uv run --env-file .env dbt docs serve --port 8083 --profiles-dir ./profiles --target dev-trino```

#### dbt test (Snowflake):
```uv run --env-file .env dbt test --profiles-dir ./profiles --target dev-snowflake --selector snowflake```
#### dbt test (Snowflake):
```uv run --env-file .env dbt test --profiles-dir ./profiles --target dev-snowflake --selector snowflake```
#### dbt test (Trino):
```uv run --env-file .env dbt test --profiles-dir ./profiles --target dev-trino --selector trino```
#### dbt run (Snowflake):
```uv run --env-file .env dbt run --profiles-dir ./profiles --target dev-snowflake --selector snowflake```
#### dbt run (Trino):
```uv run --env-file .env dbt run --profiles-dir ./profiles --target dev-trino --selector trino```

# 2. Debug
#### sync packages:
```uv sync```

#### Create Prefect block & Minio Block for connection (first run):
```uv run --env-file .env python -m scripts.s01_minio_block_create```
#### Test Snowflake connection:
```uv run --env-file .env python -m scripts.s00_test_snowflake_connection```
#### Create Prefect Variables (first run):
```uv run --env-file .env python -m scripts.s02_prefect_variables_create```
#### Prefect Deploy:
```uv run prefect deploy```
#### Prefect Worker Start:
```uv run prefect worker start --pool default```
#### dbt debug
```uv run --env-file .env dbt debug```
#### dbt run
```uv run --env-file .env dbt run```


