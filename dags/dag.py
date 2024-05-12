from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.gcs import GCSUploadOperator

import requests
from bs4 import BeautifulSoup
import os
import subprocess
from google.oauth2 import service_account
import dvc.api

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 12),
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'mlops_assignment_dag',
    default_args=default_args,
    description='MLOps Implementation with Apache Airflow',
    schedule_interval='@daily',
)

def extract_data(url, **kwargs):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = [link.get('href') for link in soup.find_all('a')]
    titles = [title.text for title in soup.find_all('h2')]
    descriptions = [description.text for description in soup.find_all('p')]
    return links, titles, descriptions

def preprocess_data(**kwargs):
    # Placeholder for data preprocessing steps
    pass

def store_data_on_google_drive(data, **kwargs):
    # Placeholder for storing data on Google Drive
    pass

def version_control_data(**kwargs):
    # Placeholder for version controlling data with DVC
    pass

# Define tasks for data extraction
with dag:
    extract_dawn_data = PythonOperator(
        task_id='extract_dawn_data',
        python_callable=extract_data,
        op_kwargs={'url': 'https://www.dawn.com/'},
    )

    extract_bbc_data = PythonOperator(
        task_id='extract_bbc_data',
        python_callable=extract_data,
        op_kwargs={'url': 'https://www.bbc.com/'},
    )

# Define task for data transformation
with dag:
    preprocess_data_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
    )

# Define task for data storage and version control
with dag:
    store_on_google_drive_task = PythonOperator(
        task_id='store_on_google_drive',
        python_callable=store_data_on_google_drive,
        provide_context=True,
    )

    version_control_task = PythonOperator(
        task_id='version_control',
        python_callable=version_control_data,
        provide_context=True,
    )

# Define task for Apache Airflow DAG development
with dag:
    # Placeholder for storing data on Google Drive
    pass

    # Placeholder for version controlling data with DVC
    pass

    # Set task dependencies
    extract_dawn_data >> preprocess_data_task >> store_on_google_drive_task >> version_control_task
    extract_bbc_data >> preprocess_data_task >> store_on_google_drive_task >> version_control_task
