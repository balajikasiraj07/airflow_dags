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
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Configure your pipeline here
PIPELINE_CONFIG = {
    'google_sheets': {
        'spreadsheet_id': '16Bt5nIVHJC9M4F-OgoQg7OTQeXANqk7gwfHVMHsVpTE',
        'range': 'Sheet1!A:A',  # Column A for number_table
        'credentials_path': '/opt/airflow/credentials/google-sheets-credentials.json'
    },
    'hive_table': {
        'database': 'default',
        'table_name': 'number_table_staging',
        'location': '/user/hive/warehouse/number_table_staging/'
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
    'google_sheets_to_hive_staging_pipeline',
    default_args=default_args,
    description='Load Google Sheets data to Hive staging table',
    schedule_interval='@daily',  # Runs daily at midnight
    catchup=False,
    tags=['google_sheets', 'hive', 'staging'],
)

def extract_from_sheets(**context):
    """
    Extract data from Google Sheets using Google Sheets API
    """
    logging.info("Starting data extraction from Google Sheets...")
    
    try:
        # Load credentials
        credentials_path = PIPELINE_CONFIG['google_sheets']['credentials_path']
        spreadsheet_id = PIPELINE_CONFIG['google_sheets']['spreadsheet_id']
        range_name = PIPELINE_CONFIG['google_sheets']['range']
        
        # Authenticate with Google Sheets API
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        credentials = Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES)
        
        # Build the service
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()
        
        # Get the data
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            logging.warning("No data found in Google Sheet")
            # Create empty dataframe with expected structure
            df = pd.DataFrame(columns=['number_table'])
        else:
            # Convert to DataFrame
            # First row might be header, so handle accordingly
            if len(values) > 0 and values[0][0] == 'number_table':
                # First row is header
                data = values[1:] if len(values) > 1 else []
            else:
                # No header, all data
                data = values
            
            # Create DataFrame
            df = pd.DataFrame(data, columns=['number_table'])
            
            # Convert to numeric, handling any non-numeric values
            df['number_table'] = pd.to_numeric(df['number_table'], errors='coerce')
            
            # Remove any rows where conversion failed (NaN values)
            df = df.dropna()
        
        # Save to CSV
        output_path = '/opt/airflow/data/sheets_data.csv'
        df.to_csv(output_path, index=False)
        
        logging.info(f"Extracted {len(df)} rows from Google Sheets")
        logging.info(f"Columns: {df.columns.tolist()}")
        logging.info(f"Sample data: {df.head().to_dict()}")
        
        # Pass information to next task
        return {
            'row_count': len(df),
            'file_path': output_path,
            'columns': df.columns.tolist()
        }
        
    except Exception as e:
        logging.error(f"Error extracting data from Google Sheets: {str(e)}")
        raise

def create_hive_staging_table(**context):
    """
    Create Hive staging table DDL
    """
    # Get data info from previous task
    task_instance = context['task_instance']
    data_info = task_instance.xcom_pull(task_ids='extract_from_sheets')
    
    # Generate Hive DDL for staging table
    hive_ddl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {PIPELINE_CONFIG['hive_table']['table_name']} (
        number_table DOUBLE
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '{PIPELINE_CONFIG['hive_table']['location']}'
    TBLPROPERTIES ('skip.header.line.count'='1');
    """
    
    logging.info("Generated Hive Staging Table DDL:")
    logging.info(hive_ddl)
    
    # In production, this would execute against your Hive server
    # For now, we'll save it as a SQL file
    with open('/opt/airflow/data/create_staging_table.sql', 'w') as f:
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
        'number_table_is_numeric': df['number_table'].dtype in ['int64', 'float64'] if 'number_table' in df.columns else True,
        'no_null_numbers': df['number_table'].notna().all() if 'number_table' in df.columns else True,
    }
    
    for check, passed in validations.items():
        if passed:
            logging.info(f"✓ Validation passed: {check}")
        else:
            logging.error(f"✗ Validation failed: {check}")
            raise ValueError(f"Data validation failed: {check}")
    
    logging.info(f"Data validation summary: {len(df)} rows, all numeric values")
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
    task_id='create_hive_staging_table',
    python_callable=create_hive_staging_table,
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
    bash_command='echo "Staging pipeline completed successfully for {{ ds }}"',
    dag=dag,
)

# Set dependencies
extract_task >> validate_task >> [create_table_task, load_task] >> success_task