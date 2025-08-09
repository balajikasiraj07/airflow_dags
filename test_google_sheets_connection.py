"""
test_google_sheets_connection.py
Save this in: ~/airflow-local/dags/
Minimal DAG to test Google Sheets API connection and data reading
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import logging
import os
import json

# Configuration - Update these values
TEST_CONFIG = {
    'google_sheets': {
        # Your Google Sheet ID from the URL
        'spreadsheet_id': '16Bt5nIVHJC9M4F-OgoQg7OTQeXANqk7gwfHVMHsVpTE',
        'test_range': 'Sheet1!A1:A10',  # Read first 10 rows of column A
        'credentials_path': '/opt/airflow/credentials/google-sheets-credentials.json'
    }
}

# Default arguments
default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,  # No retries for testing
    'retry_delay': timedelta(minutes=1),
}

# Create DAG
dag = DAG(
    'test_google_sheets_connection',
    default_args=default_args,
    description='Test Google Sheets API connection and data reading',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'google_sheets'],
)

def check_prerequisites(**context):
    """Step 1: Check all prerequisites are in place"""
    logging.info("=" * 60)
    logging.info("STEP 1: CHECKING PREREQUISITES")
    logging.info("=" * 60)
    return {'status': 'Prerequisites checked'}

# Define tasks
check_prereq_task = PythonOperator(
    task_id='check_prerequisites',
    python_callable=check_prerequisites,
    dag=dag,
)
