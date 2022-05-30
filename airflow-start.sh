#!/bin/bash
AIRFLOW_HOME=/home/airflow
airflow db init
airflow users create \
  -e $AIRFLOW_EMAIL_ID
  -f Avnish -l Yadav \
  -p $AIRFLOW_USER_PASSWORD -r $AIRFLOW_USER_ROLE \
  -u $AIRFLOW_USER_NAME

airflow webserver -p $AIRFLOW_PORT & airflow scheduler 