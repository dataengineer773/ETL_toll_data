# Import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator

# This makes scheduling easy
from airflow.utils.dates import today


# Define default DAG arguments
default_args = {
    'owner': 'dummy_name',
    'depends_on_past': False,
    'email': ['dummy_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.now(),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
)


# Task_1:Define the unzip_data task
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvf /path/to/your/datafile.tar.gz -C /path/to/destination/directory',
    dag=dag,
)

# Task_2:Define the extract_data_from_csv task
def extract_data_from_csv():
    # Path to the CSV file
    input_file = '/path/to/destination/directory/vehicle-data.csv'
    output_file = '/path/to/destination/directory/csv_data.csv'
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Extract specific columns
    df_extracted = df[['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']]
    
    # Save the extracted data to a new CSV file
    df_extracted.to_csv(output_file, index=False)
    
# Task_3:extract_data_from_csv
extract_data_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

# Task_4:Define the extract_data_from_fixed_width task
def extract_data_from_fixed_width():
    input_file = '/path/to/destination/directory/payment-data.txt'
    output_file = '/path/to/destination/directory/fixed_width_data.csv'
    colspecs = [(0, 10), (10, 20)]  # Adjust as per your file's structure
    colnames = ['Type of Payment code', 'Vehicle Code']
    df = pd.read_fwf(input_file, colspecs=colspecs, names=colnames)
    df.to_csv(output_file, index=False)

extract_data_from_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

# Task_5: Define the consolidate_data task
def consolidate_data():
    csv_file = '/path/to/destination/directory/csv_data.csv'
    fixed_width_file = '/path/to/destination/directory/fixed_width_data.csv'
    consolidated_df = pd.concat([pd.read_csv(csv_file), pd.read_csv(fixed_width_file)], ignore_index=True)
    consolidated_file = '/path/to/destination/directory/extracted_data.csv'
    consolidated_df.to_csv(consolidated_file, index=False)

consolidate_data_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

# Task_6:Define the transform_data task
def transform_data():
    input_file = '/path/to/destination/directory/extracted_data.csv'
    staging_dir = '/path/to/staging/directory/'
    output_file = staging_dir + 'transformed_data.csv'
    df = pd.read_csv(input_file)
    df['vehicle_type'] = df['vehicle_type'].str.upper()
    df.to_csv(output_file, index=False)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)
# task pipeline

 unzip_data >> extract_data_from_csv >> extract_data_from_tsv >>
  extract_data_from_fixed_width >> consolidate_data >> transform_data