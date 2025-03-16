import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
from pytz import timezone

sys.path.append('C:\\Users\\lawre\\OneDrive\\Documents\\Joebucsfan_pipeline\\dags')

from S3_upload import upload_to_s3
from snowflake_load import load_to_snowflake
from snowflake_table_read import read_from_snowflake 

# Make sure Airflow can import the scraper file (adjust path if needed):
from data_scraper import (
    scrape_articles,
    save_to_csv,
    page_numbers,
    base_url,
    client,
    lag_days
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='joebucs_pipeline',
    default_args=default_args,
    description='Scrapes JoeBucsFan data and saves to CSV',
    #schedule_interval='@daily',  # Change as needed
    #start_date=datetime(2023, 1, 1),
    schedule_interval='0 6 * * *',  # 6:00 AM daily in DAG's timezone
    start_date=datetime(2023, 1, 1, tzinfo=timezone('America/New_York')),
    catchup=False
) as dag:

    def scrape_task(**kwargs):
        # Run your scraping function
        dataset = scrape_articles(
            page_numbers=page_numbers,
            url_input=base_url,
            client=client
        )
        # Push dataset to XCom
        kwargs['ti'].xcom_push(key='dataset', value=dataset)

    def save_task(**kwargs):
        # Pull dataset from XCom
        ti = kwargs['ti']
        dataset = ti.xcom_pull(task_ids='scrape_data', key='dataset')
        
        # Generate date string
        post_date_string = (datetime.now() - timedelta(days=lag_days)).strftime('%m%d%Y')
        
        # Save articles
        save_to_csv(dataset, f'data_{post_date_string}', dataset_type="article")
        # Save comments
        save_to_csv(dataset, f'comments_{post_date_string}', dataset_type="comment")

    # Create the tasks
    scrape_data = PythonOperator(
        task_id='scrape_data',
        python_callable=scrape_task,
        provide_context=True,
    )

    save_data = PythonOperator(
        task_id='save_data',
        python_callable=save_task,
        provide_context=True,
    )

    upload_s3 = PythonOperator(
        task_id='upload_s3',
        python_callable=upload_to_s3,
        provide_context=True,  # Optional in Airflow 2.x
    )

    load_snowflake = PythonOperator(
        task_id='load_snowflake',
        python_callable=load_to_snowflake,
    )

    read_snowflake = PythonOperator(
        task_id='read_snowflake',
        python_callable=read_from_snowflake,
    )

    send_email = EmailOperator(
        task_id='send_email',
        to='lawrence.miley@yahoo.com',
        subject='Airflow DAG Notification',
        html_content='<p>The joebucsfan pipeline has been completed successfully.</p>'
    )
    
    # Define task order
    scrape_data >> save_data >> upload_s3 >> load_snowflake >> read_snowflake >> send_email