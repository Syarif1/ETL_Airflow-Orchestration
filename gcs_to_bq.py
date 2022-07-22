from typing import Any

import airflow
from airflow import models
from airflow.operators import bash_operator

gcs_to_bq = None  # type: Any
try:
    from airflow.contrib.operators import gcs_to_bq
except ImportError:
    pass


if gcs_to_bq is not None:
    args = {
        'owner': 'Airflow',
        'start_date': airflow.utils.dates.days_ago(2)
    }

    dag = models.DAG(
        dag_id='example_gcs_to_bq_operator', default_args=args,
        schedule_interval   ='@once')

    create_test_dataset = bash_operator.BashOperator(
        task_id='create_airflow_test_dataset',
        bash_command='bq mk airflow_test',
        dag=dag)
    
    sql_con_name           = 'mysql_local'
    gcs_con_name           = 'mygcp_connection'
    gcs_project_id         = 'gcp-data-eng-july-2022'
    bucket_name            = 'open-data-jakarta'
    table_name             = 'data_kk_2020'
    dataset                = 'open_data_jakarta'
    postfix                = 'x'
    folder_name            = 'Open Data Jakarta' #folder name
    file_ext               = 'csv'

    # [START howto_operator_gcs_to_bq]
    load_csv = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq_example',
        bucket='open-data-jakarta',

        source_objects                  = folder_name +'/'+ table_name  + '.' + file_ext,#
        destination_project_dataset_table =''+ gcs_project_id +'.'+ dataset +'.'+ table_name + '',

        # source_objects=['bigquery/us-states/us-states.csv'],
        # destination_project_dataset_table='airflow_test.gcs_to_bq_table',

        schema_fields=[
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'post_abbr', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
        dag=dag)
    # [END howto_operator_gcs_to_bq]

    delete_test_dataset = bash_operator.BashOperator(
        task_id='delete_airflow_test_dataset',
        bash_command='bq rm -rf airflow_test',
        dag=dag)

    create_test_dataset >> load_csv >> delete_test_dataset