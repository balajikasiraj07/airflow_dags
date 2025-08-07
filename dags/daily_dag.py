"""
google_sheets_pipeline.py
Save this in: ~/airflow-local/dags/
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import logging

# Configure your pipeline here
PIPELINE_CONFIG = {
    'google_sheets': {
        'spreadsheet_id': 'YOUR_SPREADSHEET_ID_HERE',  # Get from Google Sheets URL
        'range': 'Sheet1!A:E',  # Adjust based on your data
    },
    'hive_table': {
        'database': 'default',
        'table_name': 'your_table_name',
        'location': '/user/hive/warehouse/your_table_name/'
    }
}

# Default arguments
default_args = {
    'owner': 'analyst',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'google_sheets_to_hive_pipeline',
    default_args=default_args,
    description='Load Google Sheets data to Hive external table',
    schedule_interval='@daily',  # Runs daily at midnight
    catchup=False,
    tags=['google_sheets', 'hive'],
)

def extract_from_sheets(**context):
    """
    Extract data from Google Sheets
    For now using sample data - we'll add real Google Sheets API later
    """
    logging.info("Starting data extraction from Google Sheets...")
    
    # SAMPLE DATA - Replace with your actual columns
    sample_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Product A', 'Product B', 'Product C', 'Product D', 'Product E'],
        'category': ['Electronics', 'Clothing', 'Electronics', 'Food', 'Clothing'],
        'price': [299.99, 49.99, 199.99, 15.99, 79.99],
        'date': ['2024-01-01', '2024-01-01', '2024-01-02', '2024-01-02', '2024-01-03']
    }
    
    df = pd.DataFrame(sample_data)
    
    # Save to CSV (simulating HDFS upload)
    output_path = '/opt/airflow/data/sheets_data.csv'
    df.to_csv(output_path, index=False)
    
    logging.info(f"Extracted {len(df)} rows")
    logging.info(f"Columns: {df.columns.tolist()}")
    
    # Pass information to next task
    return {
        'row_count': len(df),
        'file_path': output_path,
        'columns': df.columns.tolist()
    }

def create_hive_table(**context):
    """
    Create Hive external table DDL
    """
    # Get data info from previous task
    task_instance = context['task_instance']
    data_info = task_instance.xcom_pull(task_ids='extract_from_sheets')
    
    # Generate Hive DDL
    hive_ddl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {PIPELINE_CONFIG['hive_table']['table_name']} (
        id INT,
        name STRING,
        category STRING,
        price DOUBLE,
        date DATE
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '{PIPELINE_CONFIG['hive_table']['location']}'
    TBLPROPERTIES ('skip.header.line.count'='1');
    """
    
    logging.info("Generated Hive DDL:")
    logging.info(hive_ddl)
    
    # In production, this would execute against your Hive server
    # For now, we'll save it as a SQL file
    with open('/opt/airflow/data/create_table.sql', 'w') as f:
        f.write(hive_ddl)
    
    return hive_ddl

def validate_data(**context):
    """
    Validate data quality
    """
    task_instance = context['task_instance']
    data_info = task_instance.xcom_pull(task_ids='extract_from_sheets')
    
    file_path = data_info['file_path']
    expected_rows = data_info['row_count']
    
    # Read and validate
    df = pd.read_csv(file_path)
    
    validations = {
        'row_count_matches': len(df) == expected_rows,
        'no_empty_dataframe': not df.empty,
        'no_null_ids': df['id'].notna().all() if 'id' in df.columns else True,
        'price_positive': (df['price'] >= 0).all() if 'price' in df.columns else True
    }
    
    for check, passed in validations.items():
        if passed:
            logging.info(f"✓ Validation passed: {check}")
        else:
            logging.error(f"✗ Validation failed: {check}")
            raise ValueError(f"Data validation failed: {check}")
    
    return "All validations passed"

def load_to_hdfs(**context):
    """
    Load data to HDFS/Storage location
    """
    task_instance = context['task_instance']
    data_info = task_instance.xcom_pull(task_ids='extract_from_sheets')
    
    file_path = data_info['file_path']
    hdfs_path = PIPELINE_CONFIG['hive_table']['location']
    
    # In production, you would run:
    # hdfs_command = f"hdfs dfs -put -f {file_path} {hdfs_path}"
    
    logging.info(f"Loading data from {file_path} to {hdfs_path}")
    logging.info("In production, this would execute HDFS commands")
    
    return f"Data loaded to {hdfs_path}"

# Define tasks
extract_task = PythonOperator(
    task_id='extract_from_sheets',
    python_callable=extract_from_sheets,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_hive_table',
    python_callable=create_hive_table,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_hdfs',
    python_callable=load_to_hdfs,
    dag=dag,
)

success_task = BashOperator(
    task_id='success_notification',
    bash_command='echo "Pipeline completed successfully for {{ ds }}"',
    dag=dag,
)

# Set dependencies
extract_task >> validate_task >> [create_table_task, load_task] >> success_task
