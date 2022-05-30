FROM  avnish327030/spark-hadoop:latest
ENV AIRFLOW_PORT=8081
ENV AIRFLOW_USER_NAME=airflow
ENV AIRFLOW_USER_PASSWORD=airflow
ENV AIRFLOW_USER_ROLE=Admin
ENV AIRFLOW_EMAIL_ID=yadav.tara.avnish@gmail.com
RUN pip install apache-airflow==2.2.4
COPY ./airflow-start.sh .
RUN chmod 777 ./airflow-start.sh
CMD ["./airflow-start.sh"]