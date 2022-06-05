#bin/bash
docker build -t spark-hadoop-airflow:latest .
PROJECT_DIR=$(pwd)

docker run -it \
    -p 9870:9870 \
    -p 8088:8088 \
    -p 8080:8080 \
    -p 18080:18080 \
    -p 9000:9000 \
    -p 8888:8888 \
    -p 9864:9864 \
    -p 8085:8085 \
    -p 8793:8793 \
    -p 8081:8081 \
    -v $PROJECT_DIR/project/notebook:/root/ipynb \
    -v $PROJECT_DIR/project/airflow:/home/airflow \
    -v $PROJECT_DIR/data:/data \
    spark-hadoop-airflow