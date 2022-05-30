#!/bin/bash
AIRFLOW_HOME=/home/airflow
airflow db init
airflow users create  -e avnish@ineuron.ai -f Avnish -l Yadav -p admin -r Admin  -u admin
airflow webserver -p $AIRFLOW_PORT & airflow scheduler 