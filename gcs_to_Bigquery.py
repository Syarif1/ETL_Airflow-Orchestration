
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Agus Syarif',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 1),
    'email': ['m.agussyarif@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'concurrency': 1,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

#run everyday
dag = DAG(
    'ct_gcs_to_bq_v2', 
    tags                = ['ct_gcs_to_bq'], 
    description         ='DAG riseloka',
    default_args        = default_args, 
    catchup             = False, 
    schedule_interval   ='@once',#timedelta(days=1)#
)

#initiate task
start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

#parameter google cloud storage bucket
sql_con_name           = 'mysql_local'
gcs_con_name           = 'mygcp_connection'
gcs_project_id         = 'gcp-data-eng-july-2022'
bucket_name            = 'open-data-jakarta'
table_name             = 'data_kk_2020'
dataset                = 'open_data_jakarta'
postfix                = 'x'
folder_name            = 'Open Data Jakarta' #folder name
file_ext               = 'csv'



load = GCSToBigQueryOperator(
    
    task_id                         = 'load_to_bq',
    bucket                          = bucket_name, 

    source_objects                  = folder_name +'/'+ table_name  + '.' + file_ext,#
    destination_project_dataset_table =''+ gcs_project_id +'.'+ dataset +'.'+ table_name + '',
    gcp_conn_id                      = gcs_con_name,
    schema_object                   = folder_name +'/'+ table_name  +'_schemas.json',
    source_format                   = file_ext,
    field_delimiter                 = ',',
    
    schema_fields=[
            {'name': 'tahun', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'provinsi', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'kabupaten_kota', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'kecamatan', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'kelurahan', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'jenis_kelamin', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'jumlah', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],

    allow_quoted_newlines           = True,
    create_disposition              = 'CREATE_IF_NEEDED',
    write_disposition               = 'WRITE_TRUNCATE',
    ignore_unknown_values           = True,
    autodetect                      = True,
    dag                             = dag
)


#dependensi
start_task >> load >> end_task
