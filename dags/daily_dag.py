"""
google_sheets_pipeline.py
Save this in: ~/airflow-local/dags/daily_dag.py
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import logging
import os
import json

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

def test_google_connection(**context):
    """
    Test Google Sheets API connection and credentials
    """
    logging.info("=" * 50)
    logging.info("TESTING GOOGLE SHEETS CONNECTION")
    logging.info("=" * 50)
    
    credentials_path = PIPELINE_CONFIG['google_sheets']['credentials_path']
    spreadsheet_id = PIPELINE_CONFIG['google_sheets']['spreadsheet_id']
    
    # Step 1: Check if credentials file exists
    if os.path.exists(credentials_path):
        logging.info(f"✓ Credentials file found at: {credentials_path}")
        
        # Check file size and permissions
        file_size = os.path.getsize(credentials_path)
        logging.info(f"  File size: {file_size} bytes")
        
        # Try to read and validate JSON
        try:
            with open(credentials_path, 'r') as f:
                cred_data = json.load(f)
                logging.info(f"  ✓ Valid JSON file")
                logging.info(f"  Project ID: {cred_data.get('project_id', 'NOT FOUND')}")
                logging.info(f"  Client Email: {cred_data.get('client_email', 'NOT FOUND')}")
        except json.JSONDecodeError as e:
            logging.error(f"  ✗ Invalid JSON in credentials file: {e}")
            raise
        except Exception as e:
            logging.error(f"  ✗ Error reading credentials file: {e}")
            raise
    else:
        logging.error(f"✗ Credentials file NOT FOUND at: {credentials_path}")
        logging.error("  Please ensure the file is mounted correctly in Docker")
        
        # Check parent directory
        parent_dir = os.path.dirname(credentials_path)
        if os.path.exists(parent_dir):
            logging.info(f"  Parent directory exists: {parent_dir}")
            logging.info(f"  Contents: {os.listdir(parent_dir)}")
        else:
            logging.error(f"  Parent directory does NOT exist: {parent_dir}")
        raise FileNotFoundError(f"Credentials file not found: {credentials_path}")
    
    # Step 2: Try to import Google libraries
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        logging.info("✓ Google API libraries imported successfully")
    except ImportError as e:
        logging.error(f"✗ Failed to import Google libraries: {e}")
        logging.error("  Run: pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client")
        raise
    
    # Step 3: Test API connection
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        credentials = Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES)
        logging.info("✓ Credentials loaded successfully")
        
        # Build the service
        service = build('sheets', 'v4', credentials=credentials)
        logging.info("✓ Google Sheets service built successfully")
        
        # Try to get spreadsheet metadata
        sheet = service.spreadsheets()
        metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
        logging.info(f"✓ Successfully connected to spreadsheet: {metadata.get('properties', {}).get('title', 'Unknown')}")
        
        # Get sheets info
        sheets = metadata.get('sheets', [])
        for s in sheets:
            props = s.get('properties', {})
            logging.info(f"  Found sheet: {props.get('title')} (ID: {props.get('sheetId')})")
        
        return {
            'status': 'success',
            'spreadsheet_title': metadata.get('properties', {}).get('title', 'Unknown'),
            'sheets_count': len(sheets)
        }
        
    except Exception as e:
        logging.error(f"✗ Failed to connect to Google Sheets API: {e}")
        logging.error(f"  Error type: {type(e).__name__}")
        logging.error("  Possible causes:")
        logging.error("  1. Service account doesn't have access to the spreadsheet")
        logging.error("  2. Spreadsheet ID is incorrect")
        logging.error("  3. API is not enabled in Google Cloud Project")
        raise

def extract_from_sheets(**context):
    """
    Extract data from Google Sheets using Google Sheets API
    """
    logging.info("=" * 50)
    logging.info("EXTRACTING DATA FROM GOOGLE SHEETS")
    logging.info("=" * 50)
    
    # First, ensure data directory exists
    data_dir = '/opt/airflow/data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        logging.info(f"Created data directory: {data_dir}")
    
    try:
        # Import Google libraries
        from google.auth.transport.requests import Request
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        
        # Load credentials
        credentials_path = PIPELINE_CONFIG['google_sheets']['credentials_path']
        spreadsheet_id = PIPELINE_CONFIG['google_sheets']['spreadsheet_id']
        range_name = PIPELINE_CONFIG['google_sheets']['range']
        
        logging.info(f"Credentials path: {credentials_path}")
        logging.info(f"Spreadsheet ID: {spreadsheet_id}")
        logging.info(f"Range: {range_name}")
        
        # Authenticate with Google Sheets API
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        credentials = Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES)
        logging.info("✓ Credentials loaded")
        
        # Build the service
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()
        logging.info("✓ Service built")
        
        # Get the data
        logging.info(f"Fetching data from range: {range_name}")
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        logging.info(f"✓ API call successful. Received {len(values)} rows")
        
        if not values:
            logging.warning("No data found in Google Sheet")
            # Create empty dataframe with expected structure
            df = pd.DataFrame(columns=['number_table'])
        else:
            # Log first few rows for debugging
            logging.info(f"First 5 rows of raw data:")
            for i, row in enumerate(values[:5]):
                logging.info(f"  Row {i}: {row}")
            
            # Check if first row is header
            if len(values) > 0 and values[0] and str(values[0][0]).lower() == 'number_table':
                logging.info("Detected header row, skipping it")
                data = values[1:] if len(values) > 1 else []
            else:
                logging.info("No header detected, using all rows as data")
                data = values
            
            # Create DataFrame - handle single column data
            # Each row might be a list with one element
            flat_data = []
            for row in data:
                if row:  # Check if row is not empty
                    flat_data.append(row[0] if isinstance(row, list) else row)
            
            df = pd.DataFrame(flat_data, columns=['number_table'])
            logging.info(f"Created DataFrame with {len(df)} rows")
            
            # Convert to numeric, handling any non-numeric values
            before_conversion = len(df)
            df['number_table'] = pd.to_numeric(df['number_table'], errors='coerce')
            
            # Remove any rows where conversion failed (NaN values)
            df = df.dropna()
            after_conversion = len(df)
            
            if before_conversion != after_conversion:
                logging.warning(f"Removed {before_conversion - after_conversion} non-numeric rows")
        
        # Save to CSV
        output_path = '/opt/airflow/data/sheets_data.csv'
        df.to_csv(output_path, index=False)
        logging.info(f"✓ Saved {len(df)} rows to {output_path}")
        
        # Log sample of final data
        if not df.empty:
            logging.info("Sample of final data:")
            logging.info(df.head().to_string())
            logging.info(f"Data statistics:")
            logging.info(f"  Min: {df['number_table'].min()}")
            logging.info(f"  Max: {df['number_table'].max()}")
            logging.info(f"  Mean: {df['number_table'].mean():.2f}")
        
        # Pass information to next task
        return {
            'row_count': len(df),
            'file_path': output_path,
            'columns': df.columns.tolist(),
            'extraction_timestamp': datetime.now().isoformat()
        }
        
    except ImportError as e:
        logging.error(f"Failed to import required libraries: {e}")
        logging.error("Please ensure google-api-python-client is installed in Docker container")
        raise
        
    except FileNotFoundError as e:
        logging.error(f"Credentials file not found: {e}")
        logging.error(f"Expected at: {credentials_path}")
        logging.error("Please ensure credentials are properly mounted in Docker")
        raise
        
    except Exception as e:
        logging.error(f"Unexpected error during extraction: {e}")
        logging.error(f"Error type: {type(e).__name__}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        raise

def create_hive_staging_table(**context):
    """
    Create Hive staging table DDL
    """
    # Get data info from previous task
    task_instance = context['task_instance']
    data_info = task_instance.xcom_pull(task_ids='extract_from_sheets')
    
    logging.info(f"Creating Hive table for {data_info['row_count']} rows")
    
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
    
    # Save DDL to file
    ddl_path = '/opt/airflow/data/create_staging_table.sql'
    with open(ddl_path, 'w') as f:
        f.write(hive_ddl)
    logging.info(f"DDL saved to: {ddl_path}")
    
    return hive_ddl

def validate_data(**context):
    """
    Validate data quality
    """
    task_instance = context['task_instance']
    data_info = task_instance.xcom_pull(task_ids='extract_from_sheets')
    
    file_path = data_info['file_path']
    expected_rows = data_info['row_count']
    
    logging.info(f"Validating data from: {file_path}")
    logging.info(f"Expected rows: {expected_rows}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found: {file_path}")
    
    # Read and validate
    df = pd.read_csv(file_path)
    
    validations = {
        'row_count_matches': len(df) == expected_rows,
        'no_empty_dataframe': not df.empty,
        'has_number_table_column': 'number_table' in df.columns,
        'number_table_is_numeric': df['number_table'].dtype in ['int64', 'float64'] if 'number_table' in df.columns else False,
        'no_null_numbers': df['number_table'].notna().all() if 'number_table' in df.columns else False,
    }
    
    validation_passed = True
    for check, passed in validations.items():
        if passed:
            logging.info(f"✓ Validation passed: {check}")
        else:
            logging.error(f"✗ Validation failed: {check}")
            validation_passed = False
    
    if not validation_passed:
        raise ValueError("Data validation failed. Check logs for details.")
    
    logging.info(f"✓ All validations passed for {len(df)} rows")
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
    logging.info(f"Command would be: hdfs dfs -put -f {file_path} {hdfs_path}")
    
    return f"Data loaded to {hdfs_path}"

# Define tasks
test_connection_task = PythonOperator(
    task_id='test_google_connection',
    python_callable=test_google_connection,
    dag=dag,
)

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

# Set dependencies - test connection first, then proceed
test_connection_task >> extract_task >> validate_task >> [create_table_task, load_task] >> success_task