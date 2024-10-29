import airflow
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocStartClusterOperator, DataprocStopClusterOperator, DataprocSubmitJobOperator

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.propnex_listing import propnex_listing
from jobs.srx_listing import srx_listing
from jobs.propnex_selenium import propnex_selenium
from jobs.srx_selenium import srx_selenium
from jobs.download_historical_data import download_data
from jobs.upload_historical_data import upload_hist_data
from schemas import SCHEMA_FIELDS_SCRAPED_DATA, SCHEMA_FIELDS_HISTORICAL_DATA

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1)
}

DATE = datetime.datetime.today().strftime('%Y-%m-%d')
PROJECT_ID = '<GCP Project ID>'
AIRFLOW_RESULT_DIR='/opt/airflow/result'
BUCKET_NAME= '<GCS Bucket Name>' # Eg. 'hdb-resale-price-data-pipeline'
GCP_CONNECTION_ID='google_cloud_default'
DATAPROC_CLUSTER_NAME='<Dataproc Cluster Name>'
PYSPARK_SCRIPT_DIR=f'gs://{BUCKET_NAME}/script'
GS_RESULT_DIR=f'gs://{BUCKET_NAME}/result'
REGION='<Region Name>' # Eg. 'asia-southeast1'
DATASET_ID = '<BigQuery Dataset ID>'
TABLE_ID = '<BigQuery Table ID>' # For scraped data
HISTORICAL_DATA_TABLE_ID = '<BigQuery Table ID>' # For historical data
HISTORICAL_DATA_COLLECTION_ID = '189'


with DAG(
    dag_id = 'hdb_resale_price_data_pipeline',
    default_args=default_args,
    schedule_interval=None,
    render_template_as_native_obj=True
) as dag:
    
    # Scrape urls - propnex
    selenium_scrape_urls_propnex = PythonOperator(
        task_id = 'selenium_scrape_urls_propnex',
        python_callable = propnex_listing,
    )

    # Upload urls to gcs - propnex
    upload_urls_propnex_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_urls_propnex_to_gcs',
        src=f'{AIRFLOW_RESULT_DIR}/propnex/{{{{ti.xcom_pull(task_ids="selenium_scrape_urls_propnex")}}}}',
        dst = 'result/propnex/urls/{{ ti.xcom_pull(task_ids="selenium_scrape_urls_propnex")}}',
        bucket=BUCKET_NAME,
        gcp_conn_id=GCP_CONNECTION_ID
    )

    # Scrape properties - propnex
    selenium_scrape_properties_propnex = PythonOperator (
        task_id = 'selenium_scrape_properties_propnex',
        python_callable = propnex_selenium,
        op_args=[f'{AIRFLOW_RESULT_DIR}/propnex/{{{{ti.xcom_pull(task_ids="selenium_scrape_urls_propnex")}}}}'],
    )

    # Upload scraped data to gcs - propnex
    upload_scraped_data_propnex_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_scraped_data_propnex_to_gcs',
        src=f'{AIRFLOW_RESULT_DIR}/propnex/{{{{ti.xcom_pull(task_ids="selenium_scrape_properties_propnex")}}}}',
        dst = 'result/propnex/scraped/{{ti.xcom_pull(task_ids="selenium_scrape_properties_propnex")}}',
        bucket=BUCKET_NAME,
        gcp_conn_id=GCP_CONNECTION_ID
    )

    # Scrape urls - srx
    selenium_scrape_urls_srx = PythonOperator(
        task_id = 'selenium_scrape_urls_srx',
        python_callable = srx_listing,
    )

    # Upload urls to gcs - srx
    upload_urls_srx_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_urls_srx_to_gcs',
        src=f'{AIRFLOW_RESULT_DIR}/srx/{{{{ti.xcom_pull(task_ids="selenium_scrape_urls_srx")}}}}',
        dst = 'result/srx/urls/{{ ti.xcom_pull(task_ids="selenium_scrape_urls_srx")}}',
        bucket=BUCKET_NAME,
        gcp_conn_id=GCP_CONNECTION_ID
    )

    # Scrape properties - srx
    selenium_scrape_properties_srx = PythonOperator (
        task_id = 'selenium_scrape_properties_srx',
        python_callable = srx_selenium,
        op_args=[f'{AIRFLOW_RESULT_DIR}/srx/{{{{ti.xcom_pull(task_ids="selenium_scrape_urls_srx")}}}}']
    )

    # Upload scraped data to gcs - srx
    upload_scraped_data_srx_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_scraped_data_srx_to_gcs',
        src=f'{AIRFLOW_RESULT_DIR}/srx/{{{{ti.xcom_pull(task_ids="selenium_scrape_properties_srx")}}}}',
        dst = 'result/srx/scraped/{{ti.xcom_pull(task_ids="selenium_scrape_properties_srx")}}',
        bucket=BUCKET_NAME,
        gcp_conn_id=GCP_CONNECTION_ID
    )

    # download historical data through API
    download_historical_data = PythonOperator (
        task_id = 'download_historical_data',
        python_callable = download_data,
        op_args=[HISTORICAL_DATA_COLLECTION_ID]
    )

    # upload historical data to gcs
    upload_historical_data_to_gcs = PythonOperator(
        task_id = 'upload_historical_data_to_gcs',
        python_callable = upload_hist_data,
        op_args = [
            BUCKET_NAME, 
            'result/historical_data/csv/',
            '{{ti.xcom_pull(task_ids="download_historical_data")}}'
        ]
    )

    # Start dataproc cluster
    start_dataproc_cluster = DataprocStartClusterOperator(
        task_id='start_dataproc_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=DATAPROC_CLUSTER_NAME,
    )

    # Transformation params - historical data
    transformation_historical_data = {
        'reference': {'project_id': PROJECT_ID},
        'placement': {'cluster_name': DATAPROC_CLUSTER_NAME},
        'pyspark_job': {
            'main_python_file_uri': f'{PYSPARK_SCRIPT_DIR}/historical_data_transformation.py',
            'args': [
                BUCKET_NAME,
                '{{ti.xcom_pull(task_ids="upload_historical_data_to_gcs")}}',
                f'{GS_RESULT_DIR}/historical_data/transformed/{DATE}_transformed'
            ]
        },
    }

    # Submit PySpark job to Dataproc - historical_data
    submit_transformation_spark_job_historical_data = DataprocSubmitJobOperator(
        task_id='submit_transformation_spark_job_historical_data',
        job=transformation_historical_data,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Transformation params - propnex
    transformation_propnex = {
        'reference': {'project_id': PROJECT_ID},
        'placement': {'cluster_name': DATAPROC_CLUSTER_NAME},
        'pyspark_job': {
            'main_python_file_uri': f'{PYSPARK_SCRIPT_DIR}/propnex_transformation.py',
            'args': [
                f'{GS_RESULT_DIR}/propnex/scraped/{{{{ti.xcom_pull(task_ids="selenium_scrape_properties_propnex")}}}}',
                f'{GS_RESULT_DIR}/propnex/transformed/{DATE}_transformed'
            ]
        },
    }

    # Submit PySpark job to Dataproc - propnex
    submit_transformation_spark_job_propnex = DataprocSubmitJobOperator(
        task_id='submit_transformation_spark_job_propnex',
        job=transformation_propnex,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Transformation params - srx
    transformation_srx = {
        'reference': {'project_id': PROJECT_ID},
        'placement': {'cluster_name': DATAPROC_CLUSTER_NAME},
        'pyspark_job': {
            'main_python_file_uri': f'{PYSPARK_SCRIPT_DIR}/srx_transformation.py',
            'args': [
                f'{GS_RESULT_DIR}/srx/scraped/{{{{ti.xcom_pull(task_ids="selenium_scrape_properties_srx")}}}}',
                f'{GS_RESULT_DIR}/srx/transformed/{DATE}_transformed'
            ]
        },
    }

    # Submit PySpark job to Dataproc - srx
    submit_transformation_spark_job_srx = DataprocSubmitJobOperator(
        task_id='submit_transformation_spark_job_srx',
        job=transformation_srx,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Merge and dedup task params
    merge_dedup = {
        'reference': {'project_id': PROJECT_ID},
        'placement': {'cluster_name': DATAPROC_CLUSTER_NAME},
        'pyspark_job': {
            'main_python_file_uri': f'{PYSPARK_SCRIPT_DIR}/merge_dedup.py',
            'args': [
                f'{GS_RESULT_DIR}/propnex/transformed/{DATE}_transformed',
                f'{GS_RESULT_DIR}/srx/transformed/{DATE}_transformed',
                f'{GS_RESULT_DIR}/merged/{DATE}_merged_dedup_data',
            ]
        },
    }

    # Submit PySpark job to Dataproc to merge and dedup data
    submit_merge_dedup_spark_job = DataprocSubmitJobOperator(
        task_id='submit_merge_dedup_spark_job',
        job=merge_dedup,
        region=REGION,
        project_id=PROJECT_ID,
    )


    # Create table for historical_data (if not exists)
    create_bq_table_historical_data = BigQueryCreateEmptyTableOperator(
        task_id='create_bq_table_historical_data',
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=HISTORICAL_DATA_TABLE_ID,
        schema_fields=SCHEMA_FIELDS_HISTORICAL_DATA,
        time_partitioning={
            'type': 'DAY',
            'field': 'date_of_sale',
        },
        exists_ok=True,
    )

    # Load the transformed Parquet file into BigQuery - historical data
    load_historical_data_to_bigquery = GCSToBigQueryOperator(
        task_id='load_historical_data_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=[f'result/historical_data/transformed/{DATE}_transformed/*.parquet'],
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.{HISTORICAL_DATA_TABLE_ID}',
        source_format='PARQUET',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
    )

    # Create table for merged data - scraped data (if not exists)
    create_bq_table_scraped_data = BigQueryCreateEmptyTableOperator(
        task_id='create_bq_table_scraped_data',
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        schema_fields=SCHEMA_FIELDS_SCRAPED_DATA,
        time_partitioning={
            'type': 'DAY',
            'field': 'transformed_date', 
        },
        exists_ok=True,
    )

    # Load the transformed Parquet file into BigQuery - scraped data
    load_scraped_data_to_bigquery = GCSToBigQueryOperator(
        task_id='load_scraped_data_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=[f'result/merged/{DATE}_merged_dedup_data/*.parquet'],
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}',
        source_format='PARQUET',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
    )

    stop_dataproc_cluster = DataprocStopClusterOperator(
        task_id='stop_dataproc_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=DATAPROC_CLUSTER_NAME,
        )


    selenium_scrape_urls_propnex >> upload_urls_propnex_to_gcs >> selenium_scrape_properties_propnex >> upload_scraped_data_propnex_to_gcs >> start_dataproc_cluster
    selenium_scrape_urls_srx >> upload_urls_srx_to_gcs >> selenium_scrape_properties_srx >> upload_scraped_data_srx_to_gcs >> start_dataproc_cluster
    download_historical_data >> upload_historical_data_to_gcs >> start_dataproc_cluster
    start_dataproc_cluster >> [submit_transformation_spark_job_propnex, submit_transformation_spark_job_srx, submit_transformation_spark_job_historical_data]
    [submit_transformation_spark_job_propnex, submit_transformation_spark_job_srx] >> submit_merge_dedup_spark_job >> create_bq_table_scraped_data >> load_scraped_data_to_bigquery 
    submit_transformation_spark_job_historical_data >> create_bq_table_historical_data >> load_historical_data_to_bigquery 
    [load_scraped_data_to_bigquery, load_historical_data_to_bigquery] >> stop_dataproc_cluster
