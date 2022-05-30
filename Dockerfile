FROM  avnish327030/spark-hadoop:latest
ENV AIRFLOW_PORT=8081
RUN pip install apache-airflow==2.2.4
COPY ./airflow-start.sh .
RUN chmod 777 ./airflow-start.sh
CMD ["./airflow-start.sh"]