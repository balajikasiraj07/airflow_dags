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
    """
    Step 1: Check all prerequisites are in place
    """
    logging.info("=" * 60)
    logging.info("STEP 1: CHECKING PREREQUISITES")
    logging.info("=" * 60)
    
    results = {
        'credentials_file': False,
        'credentials_valid': False,
        'libraries_installed': False
    }
    
    # Check credentials file
    credentials_path = TEST_CONFIG['google_sheets']['credentials_path']
    logging.info(f"\n1. Checking credentials file...")
    logging.info(f"   Path: {credentials_path}")
    
    if os.path.exists(credentials_path):
        logging.info(f"   ✓ File exists")
        results['credentials_file'] = True
        
        # Validate JSON
        try:
            with open(credentials_path, 'r') as f:
                cred_data = json.load(f)
                logging.info(f"   ✓ Valid JSON format")
                logging.info(f"   Project ID: {cred_data.get('project_id', 'NOT FOUND')}")
                logging.info(f"   Client Email: {cred_data.get('client_email', 'NOT FOUND')}")
                results['credentials_valid'] = True
        except json.JSONDecodeError as e:
            logging.error(f"   ✗ Invalid JSON: {e}")
        except Exception as e:
            logging.error(f"   ✗ Error reading file: {e}")
    else:
        logging.error(f"   ✗ File NOT FOUND")
        # Check parent directory
        parent_dir = os.path.dirname(credentials_path)
        if os.path.exists(parent_dir):
            logging.info(f"   Directory exists: {parent_dir}")
            logging.info(f"   Contents: {os.listdir(parent_dir)}")
        else:
            logging.error(f"   Directory NOT exists: {parent_dir}")
    
    # Check Google libraries
    logging.info(f"\n2. Checking Google API libraries...")
    try:
        import google.auth
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        logging.info(f"   ✓ All required libraries installed")
        results['libraries_installed'] = True
    except ImportError as e:
        logging.error(f"   ✗ Missing library: {e}")
        logging.error(f"   Run: pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client")
    
    # Summary
    logging.info(f"\n" + "=" * 60)
    logging.info("PREREQUISITES SUMMARY:")
    for key, value in results.items():
        status = "✓ PASS" if value else "✗ FAIL"
        logging.info(f"  {key}: {status}")
    
    if not all(results.values()):
        raise Exception("Prerequisites check failed. See logs above for details.")
    
    return results

def test_api_connection(**context):
    """
    Step 2: Test Google Sheets API connection
    """
    logging.info("=" * 60)
    logging.info("STEP 2: TESTING API CONNECTION")
    logging.info("=" * 60)
    
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    
    credentials_path = TEST_CONFIG['google_sheets']['credentials_path']
    spreadsheet_id = TEST_CONFIG['google_sheets']['spreadsheet_id']
    
    try:
        # Load credentials
        logging.info(f"\n1. Loading credentials...")
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        credentials = Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES
        )
        logging.info(f"   ✓ Credentials loaded")
        
        # Build service
        logging.info(f"\n2. Building Google Sheets service...")
        service = build('sheets', 'v4', credentials=credentials)
        logging.info(f"   ✓ Service built")
        
        # Get spreadsheet metadata
        logging.info(f"\n3. Fetching spreadsheet metadata...")
        logging.info(f"   Spreadsheet ID: {spreadsheet_id}")
        
        sheet = service.spreadsheets()
        metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
        
        spreadsheet_title = metadata.get('properties', {}).get('title', 'Unknown')
        logging.info(f"   ✓ Connected to: '{spreadsheet_title}'")
        
        # List all sheets
        sheets = metadata.get('sheets', [])
        logging.info(f"\n4. Available sheets ({len(sheets)} found):")
        for s in sheets:
            props = s.get('properties', {})
            sheet_name = props.get('title')
            sheet_id = props.get('sheetId')
            row_count = props.get('gridProperties', {}).get('rowCount', 0)
            col_count = props.get('gridProperties', {}).get('columnCount', 0)
            logging.info(f"   - {sheet_name} (ID: {sheet_id}, Size: {row_count}x{col_count})")
        
        return {
            'connected': True,
            'spreadsheet_title': spreadsheet_title,
            'sheets_count': len(sheets),
            'service': service  # Pass service to next task
        }
        
    except Exception as e:
        logging.error(f"\n✗ API Connection Failed: {e}")
        logging.error(f"Error type: {type(e).__name__}")
        logging.error("\nPossible causes:")
        logging.error("1. Service account doesn't have access to the spreadsheet")
        logging.error("2. Spreadsheet ID is incorrect")
        logging.error("3. Google Sheets API not enabled in Google Cloud Project")
        logging.error("4. Network/firewall issues")
        raise

def read_sample_data(**context):
    """
    Step 3: Read sample data from the sheet
    """
    logging.info("=" * 60)
    logging.info("STEP 3: READING SAMPLE DATA")
    logging.info("=" * 60)
    
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    
    credentials_path = TEST_CONFIG['google_sheets']['credentials_path']
    spreadsheet_id = TEST_CONFIG['google_sheets']['spreadsheet_id']
    test_range = TEST_CONFIG['google_sheets']['test_range']
    
    try:
        # Rebuild service (since we can't pass complex objects between tasks)
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        credentials = Credentials.from_service_account_file(
            credentials_path, scopes=SCOPES
        )
        service = build('sheets', 'v4', credentials=credentials)
        
        # Read data
        logging.info(f"\n1. Reading data from range: {test_range}")
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=test_range
        ).execute()
        
        values = result.get('values', [])
        logging.info(f"   ✓ Data fetched: {len(values)} rows")
        
        if not values:
            logging.warning("   ⚠ No data found in specified range")
            return {'row_count': 0, 'sample_saved': False}
        
        # Display raw data
        logging.info(f"\n2. Raw data (first 10 rows):")
        for i, row in enumerate(values[:10], 1):
            logging.info(f"   Row {i}: {row}")
        
        # Process data
        logging.info(f"\n3. Processing data...")
        
        # Check for header
        has_header = False
        if values[0] and str(values[0][0]).lower() in ['number_table', 'number', 'value']:
            has_header = True
            logging.info(f"   Header detected: {values[0]}")
            data_rows = values[1:] if len(values) > 1 else []
        else:
            data_rows = values
        
        # Create DataFrame
        flat_data = []
        for row in data_rows:
            if row:  # Skip empty rows
                flat_data.append(row[0] if isinstance(row, list) else row)
        
        df = pd.DataFrame(flat_data, columns=['number_table'])
        logging.info(f"   Created DataFrame: {len(df)} rows")
        
        # Convert to numeric
        df['number_table'] = pd.to_numeric(df['number_table'], errors='coerce')
        valid_rows = df['number_table'].notna().sum()
        logging.info(f"   Valid numeric values: {valid_rows}/{len(df)}")
        
        # Display statistics
        if valid_rows > 0:
            logging.info(f"\n4. Data Statistics:")
            logging.info(f"   Min: {df['number_table'].min()}")
            logging.info(f"   Max: {df['number_table'].max()}")
            logging.info(f"   Mean: {df['number_table'].mean():.2f}")
            logging.info(f"   Count: {valid_rows}")
        
        # Save sample
        output_path = '/opt/airflow/data/test_sample.csv'
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        logging.info(f"\n5. Sample saved to: {output_path}")
        
        return {
            'row_count': len(values),
            'valid_rows': valid_rows,
            'has_header': has_header,
            'sample_saved': True,
            'file_path': output_path
        }
        
    except Exception as e:
        logging.error(f"\n✗ Failed to read data: {e}")
        logging.error(f"Error type: {type(e).__name__}")
        raise

def generate_summary(**context):
    """
    Step 4: Generate test summary
    """
    logging.info("=" * 60)
    logging.info("FINAL TEST SUMMARY")
    logging.info("=" * 60)
    
    # Gather results from previous tasks
    task_instance = context['task_instance']
    prerequisites = task_instance.xcom_pull(task_ids='check_prerequisites')
    connection = task_instance.xcom_pull(task_ids='test_api_connection')
    data = task_instance.xcom_pull(task_ids='read_sample_data')
    
    logging.info("\n✅ TEST RESULTS:")
    logging.info(f"\n1. Prerequisites:")
    logging.info(f"   - Credentials file: ✓")
    logging.info(f"   - Valid JSON: ✓")
    logging.info(f"   - Libraries installed: ✓")
    
    logging.info(f"\n2. API Connection:")
    logging.info(f"   - Connected to: {connection['spreadsheet_title']}")
    logging.info(f"   - Sheets found: {connection['sheets_count']}")
    
    logging.info(f"\n3. Data Reading:")
    logging.info(f"   - Rows fetched: {data['row_count']}")
    logging.info(f"   - Valid numeric rows: {data['valid_rows']}")
    logging.info(f"   - Has header: {data['has_header']}")
    logging.info(f"   - Sample saved: {data['sample_saved']}")
    
    logging.info("\n" + "=" * 60)
    logging.info("🎉 ALL TESTS PASSED SUCCESSFULLY!")
    logging.info("Your Google Sheets connection is working properly.")
    logging.info("You can now proceed with the main pipeline.")
    logging.info("=" * 60)
    
    return "Test completed successfully"

# Define tasks
check_prereq_task = PythonOperator(
    task_id='check_prerequisites',
    python_callable=check_prerequisites,
    dag=dag,
)

test_connection_task = PythonOperator(
    task_id='test_api_connection',
    python_callable=test_api_connection,
    dag=dag,
)

read_data_task = PythonOperator(
    task_id='read_sample_data',
    python_callable=read_sample_data,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary,
    dag=dag,
)

# Set dependencies - sequential execution
check_prereq_task >> test_connection_task >> read_data_task >> summary_task