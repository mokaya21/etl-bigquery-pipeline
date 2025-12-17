from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from google.cloud import bigquery
import logging

# Configuration
PROJECT_ID = "etl-simon-001-479300"
DATASET_ID = "staging_dataset"

# File paths - Local paths used since files are on Airflow server
EXCEL_FILE_PATH ="/home/simon_mokaya/airflow/data/Store_sales.xlsx"
JSON_FILE_PATH  ="/home/simon_mokaya/airflow/data/products.json"

# Default arguments with retry logic and alerting
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 3),
    'retries': 3,  # Retry 3 times on failure
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes between retries
    'email': ['mokayasimon495@gmail.com'],
    'email_on_failure': True,  # Send email when task fails
    'email_on_retry': False,  # Don't send email on retry
    'email_on_success': False,  # Optional: notify on success
}

# Instantiate the DAG
dag = DAG(
    dag_id='etl_pipeline_workflow',
    default_args=default_args,
    description='Complete ETL pipeline: Extract, Transform, Load to BigQuery',
    schedule='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['etl', 'bigquery', 'daily', 'staging'],
)

# ============ SETUP TASK ============

def create_bigquery_dataset():
    """Create BigQuery dataset if it doesn't exist"""
    try:
        logging.info(f"Creating/verifying BigQuery dataset: {DATASET_ID}")

        client = bigquery.Client(project=PROJECT_ID)
        dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
        dataset = bigquery.Dataset(dataset_ref)

        # Set location for dataset
        dataset.location = "us-central1"

        # Create dataset if it doesn't exist
        dataset = client.create_dataset(dataset, exists_ok=True)

        logging.info(f"Dataset '{DATASET_ID}' is ready")
        return "Dataset created/verified successfully"
    except Exception as e:
        logging.error(f"Dataset creation failed: {str(e)}")
        raise

# ============ EXTRACT TASKS ============

def extract_excel_data(**context):
    """Extract data from Excel file"""
    try:
        logging.info(f"Starting Excel extraction from: {EXCEL_FILE_PATH}")

        # Read Excel file
        df = pd.read_excel(EXCEL_FILE_PATH)

        # Save to temporary location for next task
        temp_path = '/tmp/extracted_excel.csv'
        df.to_csv(temp_path, index=False)

        # Push file path to XCom for next task
        context['ti'].xcom_push(key='excel_temp_path', value=temp_path)
        context['ti'].xcom_push(key='excel_row_count', value=len(df))

        logging.info(f"Successfully extracted {len(df)} rows from Excel")
        return temp_path
    except Exception as e:
        logging.error(f"Excel extraction failed: {str(e)}")
        raise

def extract_json_data(**context):
    """Extract data from JSON file"""
    try:
        logging.info(f"Starting JSON extraction from: {JSON_FILE_PATH}")

        # Read JSON file
        df = pd.read_json(JSON_FILE_PATH)

        # Save to temporary location
        temp_path = '/tmp/extracted_json.csv'
        df.to_csv(temp_path, index=False)

        # Push to XCom
        context['ti'].xcom_push(key='json_temp_path', value=temp_path)
        context['ti'].xcom_push(key='json_row_count', value=len(df))

        logging.info(f"Successfully extracted {len(df)} rows from JSON")
        return temp_path
    except Exception as e:
        logging.error(f"JSON extraction failed: {str(e)}")
        raise

# ============ TRANSFORM TASKS ============

def transform_excel_data(**context):
    """Transform Excel data - clean columns and cast data types"""
    try:
        logging.info("Starting Excel transformation...")

        # Get extracted file path from XCom
        excel_path = context['ti'].xcom_pull(key='excel_temp_path', task_ids='extract_excel')
        df = pd.read_csv(excel_path)

        logging.info(f"Initial row count: {len(df)}")

        # Check if the first column contains comma-separated values
        first_col = df.columns[0]
        sample = df[first_col].dropna().astype(str).head(5)

        if sample.str.contains(",").any():
            # Split the first column
            logging.info("Detected comma-separated values, splitting...")
            df = df[first_col].str.split(",", expand=True)
            df.columns = ["date", "store_id", "product_id", "units_sold", "sales_amount"]
        else:
            # Rename columns to expected names
            expected_cols = ["date", "store_id", "product_id", "units_sold", "sales_amount"]
            df = df.rename(columns={df.columns[i]: expected_cols[i] for i in range(len(expected_cols))})

        # Cast columns to correct data types
        logging.info("Casting columns to appropriate data types...")
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["units_sold"] = pd.to_numeric(df["units_sold"], errors="coerce").astype("int64")
        df["sales_amount"] = pd.to_numeric(df["sales_amount"], errors="coerce").astype("float64")

        # Remove rows with null values after transformation
        initial_count = len(df)
        df = df.dropna()
        removed_count = initial_count - len(df)

        if removed_count > 0:
            logging.warning(f"Removed {removed_count} rows with null values")

        # Save transformed data
        transformed_path = '/tmp/transformed_excel.csv'
        df.to_csv(transformed_path, index=False)

        context['ti'].xcom_push(key='excel_transformed_path', value=transformed_path)
        context['ti'].xcom_push(key='excel_final_count', value=len(df))

        logging.info(f"Successfully transformed {len(df)} rows from Excel data")
        return transformed_path
    except Exception as e:
        logging.error(f"Excel transformation failed: {str(e)}")
        raise

def transform_json_data(**context):
    """Transform JSON data - cast data types and clean"""
    try:
        logging.info("Starting JSON transformation...")

        # Get extracted file path from XCom
        json_path = context['ti'].xcom_pull(key='json_temp_path', task_ids='extract_json')
        df = pd.read_csv(json_path)

        logging.info(f"Initial row count: {len(df)}")

        # Change data type for price column
        df["price"] = pd.to_numeric(df["price"], errors="coerce").astype("float64")

        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates()
        removed_count = initial_count - len(df)

        if removed_count > 0:
            logging.warning(f"Removed {removed_count} duplicate rows")

        # Remove rows with null values
        df = df.dropna()

        # Save transformed data
        transformed_path = '/tmp/transformed_json.csv'
        df.to_csv(transformed_path, index=False)

        context['ti'].xcom_push(key='json_transformed_path', value=transformed_path)
        context['ti'].xcom_push(key='json_final_count', value=len(df))

        logging.info(f"Successfully transformed {len(df)} rows from JSON data")
        return transformed_path
    except Exception as e:
        logging.error(f"JSON transformation failed: {str(e)}")
        raise

# ============ LOAD TASKS ============

def load_staging_table(df, table_name):
    """
    Reusable function to load a DataFrame to BigQuery staging table
    """
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True  # Auto-detect schema
    )

    client = bigquery.Client(project=PROJECT_ID)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    logging.info(f"Loaded {len(df)} rows into table '{table_name}'")
    return table_id

def load_excel_to_bigquery(**context):
    """Load transformed Excel data to BigQuery staging table"""
    try:
        logging.info("Starting Excel data load to BigQuery...")

        # Get transformed file path from XCom
        transformed_path = context['ti'].xcom_pull(key='excel_transformed_path', task_ids='transform_excel')
        df = pd.read_csv(transformed_path)

        # Parse date column back to datetime
        df["date"] = pd.to_datetime(df["date"])

        # Load to BigQuery using reusable function
        table_id = load_staging_table(df, "store_sales")

        logging.info(f"Successfully loaded {len(df)} rows to {table_id}")
        return f"Success: {len(df)} rows loaded to store_sales"
    except Exception as e:
        logging.error(f"Excel load to BigQuery failed: {str(e)}")
        raise

def load_json_to_bigquery(**context):
    """Load transformed JSON data to BigQuery staging table"""
    try:
        logging.info("Starting JSON data load to BigQuery...")

        # Get transformed file path from XCom
        transformed_path = context['ti'].xcom_pull(key='json_transformed_path', task_ids='transform_json')
        df = pd.read_csv(transformed_path)

        # Load to BigQuery using reusable function
        table_id = load_staging_table(df, "products")

        logging.info(f"Successfully loaded {len(df)} rows to {table_id}")
        return f"Success: {len(df)} rows loaded to products"
    except Exception as e:
        logging.error(f"JSON load to BigQuery failed: {str(e)}")
        raise

# ============ VALIDATION TASK ============

def validate_data(**context):
    """
    Run data quality checks on loaded BigQuery tables.
    Validates: row counts, nulls, duplicates, and referential integrity.
    """
    client = bigquery.Client(project=PROJECT_ID)
    validation_results = []
    has_critical_failure = False

    try:
        logging.info("="*60)
        logging.info("Starting Data Quality Validation")
        logging.info("="*60)

        # Get expected row counts from XCom
        expected_sales_count = context['ti'].xcom_pull(key='excel_final_count', task_ids='transform_excel')
        expected_products_count = context['ti'].xcom_pull(key='json_final_count', task_ids='transform_json')

        # ============ 1. ROW COUNT CHECKS ============

        logging.info("Checking row counts...")

        sales_count = client.query(f"""
            SELECT COUNT(*) as cnt
            FROM `{PROJECT_ID}.{DATASET_ID}.store_sales`
        """).result().to_dataframe().iloc[0]['cnt']

        products_count = client.query(f"""
            SELECT COUNT(*) as cnt
            FROM `{PROJECT_ID}.{DATASET_ID}.products`
        """).result().to_dataframe().iloc[0]['cnt']

        # Check if tables are empty
        if sales_count == 0:
            has_critical_failure = True
            validation_results.append("❌ CRITICAL: store_sales table is empty")
        else:
            validation_results.append(f"✅ store_sales has {sales_count} rows")

        if products_count == 0:
            has_critical_failure = True
            validation_results.append("❌ CRITICAL: products table is empty")
        else:
            validation_results.append(f"✅ products has {products_count} rows")

        # Verify row counts match expected from transformation
        if sales_count != expected_sales_count:
            has_critical_failure = True
            validation_results.append(
                f"❌ CRITICAL: store_sales row count mismatch. Expected: {expected_sales_count}, Got: {sales_count}"
            )
        else:
            validation_results.append(f"✅ store_sales row count matches transformed data")

        if products_count != expected_products_count:
            has_critical_failure = True
            validation_results.append(
                f"❌ CRITICAL: products row count mismatch. Expected: {expected_products_count}, Got: {products_count}"
            )
        else:
            validation_results.append(f"✅ products row count matches transformed data")

        # ============ 2. NULL CHECKS ============

        logging.info("Checking for null values in critical columns...")

        null_check_sales = client.query(f"""
            SELECT
                COUNTIF(date IS NULL) as null_dates,
                COUNTIF(product_id IS NULL) as null_product_ids,
                COUNTIF(units_sold IS NULL) as null_units,
                COUNTIF(sales_amount IS NULL) as null_amounts
            FROM `{PROJECT_ID}.{DATASET_ID}.store_sales`
        """).result().to_dataframe().iloc[0]

        for col, null_count in null_check_sales.items():
            col_name = col.replace('null_', '')
            if null_count > 0:
                has_critical_failure = True
                validation_results.append(f"❌ CRITICAL: {null_count} null values in store_sales.{col_name}")
            else:
                validation_results.append(f"✅ No nulls in store_sales.{col_name}")

        null_check_products = client.query(f"""
            SELECT
                COUNTIF(product_id IS NULL) as null_product_ids,
                COUNTIF(product_name IS NULL) as null_names,
                COUNTIF(price IS NULL) as null_prices
            FROM `{PROJECT_ID}.{DATASET_ID}.products`
        """).result().to_dataframe().iloc[0]

        for col, null_count in null_check_products.items():
            col_name = col.replace('null_', '')
            if null_count > 0:
                has_critical_failure = True
                validation_results.append(f"❌ CRITICAL: {null_count} null values in products.{col_name}")
            else:
                validation_results.append(f"✅ No nulls in products.{col_name}")

        # ============ 3. DUPLICATE CHECKS ============

        logging.info("Checking for duplicate records...")

        duplicates_sales = client.query(f"""
            SELECT product_id, date, COUNT(*) as dupes
            FROM `{PROJECT_ID}.{DATASET_ID}.store_sales`
            GROUP BY product_id, date
            HAVING COUNT(*) > 1
        """).result().to_dataframe()

        if len(duplicates_sales) > 0:
            has_critical_failure = True
            validation_results.append(f"❌ CRITICAL: {len(duplicates_sales)} duplicate records in store_sales")
            logging.warning(f"Duplicate records:\n{duplicates_sales.head()}")
        else:
            validation_results.append("✅ No duplicates in store_sales")

        duplicates_products = client.query(f"""
            SELECT product_id, COUNT(*) as dupes
            FROM `{PROJECT_ID}.{DATASET_ID}.products`
            GROUP BY product_id
            HAVING COUNT(*) > 1
        """).result().to_dataframe()

        if len(duplicates_products) > 0:
            has_critical_failure = True
            validation_results.append(f"❌ CRITICAL: {len(duplicates_products)} duplicate product_ids in products table")
            logging.warning(f"Duplicate products:\n{duplicates_products.head()}")
        else:
            validation_results.append("✅ No duplicates in products")

        # ============ 4. REFERENTIAL INTEGRITY ============

        logging.info("Checking referential integrity...")

        orphaned = client.query(f"""
            SELECT COUNT(*) as orphaned_records
            FROM `{PROJECT_ID}.{DATASET_ID}.store_sales` s
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.products` p
            ON s.product_id = p.product_id
            WHERE p.product_id IS NULL
        """).result().to_dataframe().iloc[0]['orphaned_records']

        if orphaned > 0:
            has_critical_failure = True
            validation_results.append(f"❌ CRITICAL: {orphaned} sales records with invalid product_ids")
        else:
            validation_results.append("✅ All sales records have valid product_ids")

        # ============ 5. BUSINESS LOGIC VALIDATION ============

        logging.info("Validating business rules...")

        ranges = client.query(f"""
            SELECT
                MIN(sales_amount) as min_amount,
                MAX(sales_amount) as max_amount,
                MIN(units_sold) as min_units,
                MAX(units_sold) as max_units
            FROM `{PROJECT_ID}.{DATASET_ID}.store_sales`
        """).result().to_dataframe().iloc[0]

       # Check for negative sales amounts
        if ranges['min_amount'] < 0:
            has_critical_failure = True
            validation_results.append(f"❌ CRITICAL: Negative sales_amount detected: {ranges['min_amount']}")
        else:
            validation_results.append(f"✅ sales_amount valid range: ${ranges['min_amount']:.2f} - ${ranges['max_amount']:.2f}")

        # Check for negative units
        if ranges['min_units'] < 0:
            has_critical_failure = True
            validation_results.append(f"❌ CRITICAL: Negative units_sold detected: {ranges['min_units']}")
        else:
            validation_results.append(f"✅ units_sold valid range: {ranges['min_units']} - {ranges['max_units']}")

        # Check product prices
        price_ranges = client.query(f"""
            SELECT
                MIN(price) as min_price,
                MAX(price) as max_price
            FROM `{PROJECT_ID}.{DATASET_ID}.products`
        """).result().to_dataframe().iloc[0]

        if price_ranges['min_price'] <= 0:
            has_critical_failure = True
            validation_results.append(f"❌ CRITICAL: Invalid product price detected: ${price_ranges['min_price']}")
        else:
            validation_results.append(f"✅ product prices valid range: ${price_ranges['min_price']:.2f} - ${price_ranges['max_price']:.2f}")

        # ============ GENERATE REPORT ============

        report = "\n".join(validation_results)
        logging.info("\n" + "="*60)
        logging.info("DATA QUALITY REPORT")
        logging.info("="*60)
        logging.info(report)
        logging.info("="*60)

        # Store results in XCom for potential email alerts
        context['ti'].xcom_push(key='validation_report', value=report)
        context['ti'].xcom_push(key='has_critical_failure', value=has_critical_failure)

        # Fail the task if critical issues found
        if has_critical_failure:
            raise ValueError(f"Data validation failed with critical errors:\n{report}")

        logging.info("✅ All data quality checks passed!")
        return "All validation checks passed successfully"

    except Exception as e:
        logging.error(f"Validation failed: {str(e)}")
        raise

# ============ DEFINE TASKS ============

# Setup task - Create BigQuery dataset
create_dataset = PythonOperator(
    task_id='create_bigquery_dataset',
    python_callable=create_bigquery_dataset,
    dag=dag,
)

# Extract tasks
extract_excel = PythonOperator(
    task_id='extract_excel',
    python_callable=extract_excel_data,
    dag=dag,
)

extract_json = PythonOperator(
    task_id='extract_json',
    python_callable=extract_json_data,
    dag=dag,
)

# Transform tasks
transform_excel = PythonOperator(
    task_id='transform_excel',
    python_callable=transform_excel_data,
    dag=dag,
)

transform_json = PythonOperator(
    task_id='transform_json',
    python_callable=transform_json_data,
    dag=dag,
)

# Load tasks
load_excel = PythonOperator(
    task_id='load_excel_to_bigquery',
    python_callable=load_excel_to_bigquery,
    dag=dag,
)

load_json = PythonOperator(
    task_id='load_json_to_bigquery',
    python_callable=load_json_to_bigquery,
    dag=dag,
)

# Validation task
validate = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

# ============ SET TASK DEPENDENCIES ============

# First, create the dataset
# Then run both pipelines in parallel
create_dataset >> [extract_excel, extract_json]

# Excel pipeline: extract -> transform -> load
extract_excel >> transform_excel >> load_excel

# JSON pipeline: extract -> transform -> load
extract_json >> transform_json >> load_json

# After loading the transformed data, run the data validation step
[load_excel, load_json] >> validate
