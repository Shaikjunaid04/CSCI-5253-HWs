from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from Scripts.etl import retrieve_data, transform_data, load_data
from datetime import datetime, timedelta
import pandas as pd
import requests
import boto3
import requests
import io
import time
import logging
from sqlalchemy import create_engine

import sys
from dotenv import load_dotenv
import os
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning) 
'''
load_dotenv("/Users/ashkans/Documents/datacenter/A-3/dags/secrets.env")




URL = os.getenv('URL')
YOUR_ACCESS_KEY = os.getenv('YOUR_ACCESS_KEY')
YOUR_SECRET_KEY = os.getenv('YOUR_SECRET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
DATABASE_URL = os.getenv('DATABASE_URL')


URL="https://data.austintexas.gov/resource/9t4d-g238.json"
YOUR_ACCESS_KEY="AKIAZKUHUNYSBK6XPTVB"
YOUR_SECRET_KEY="Bf31iAqYPaHYNicudGAHaxxibpMtfoakdaXLl0KX"
BUCKET_NAME="airflow-dcsc-bucket-1"
POSTGRES_PORT="5432"
POSTGRES_USER="airflow"
POSTGRES_PASSWORD="ssrk-12345"
POSTGRES_DB="postgres"
POSTGRES_HOST="shelter-airflow.c4ar66xddoql.us-east-1.rds.amazonaws.com"
DATABASE_URL = "postgresql+psycopg2://{}:{}@{}:{}/{}"

DATABASE_URL = DATABASE_URL.format(POSTGRES_USER,POSTGRES_PASSWORD,POSTGRES_HOST,POSTGRES_PORT,POSTGRES_DB)

'''

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('austin_animal_shelter_dag',
          default_args=default_args,
          description='DAG for Austin Animal Shelter Data',
          schedule_interval=timedelta(days=1))


retrieve_data_task = PythonOperator(
    task_id='retrieve_data',
    python_callable=retrieve_data,
    dag=dag)



transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag)


load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag)



retrieve_data_task >> transform_data_task >> load_data_task


