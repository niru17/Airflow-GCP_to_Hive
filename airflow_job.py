from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from google.cloud import storage
from datetime import datetime, timedelta
from airflow.models.param import Param

default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'email_on_retry':False,
    'email_on_failure':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'start_date':datetime(2025,5,5),
}

dag=DAG(
    "json_write_to_hive",
    default_args=default_args,
    description="A DAG to run integrate Airflow and Hive",
    catchup=False,
    schedule_interval=timedelta(days=1),
    tags=['dev'],
    params={
        'execution_date':Param(default='NA', type='string', description="Execution date in yyyymmdd format")
    },
)

def execution_date(ds_nodash,**kwargs):
    execution_date=kwargs['params'].get('execution_date','NA')
    if execution_date=='NA':
        execution_date=ds_nodash
    return execution_date

python_get_execution_date=PythonOperator(
    task_id="Execution_date",
    python_callable=execution_date,
    provide_context=True,
    op_kwargs={'ds_nodash':'{{ds_nodash}}'},
    do_xcom_push=True,
    dag=dag,
)

CLUSTER_NAME='dataproc-airflow-assignment'
REGION='us-central1'
PROJECT_ID='fit-legacy-454720-g4'
CLUSTER_CONFIG={
    'master_config':{
        'num_instances':1,
        'machine_type_uri':'n1-standard-2',
        'disk_config':{
            'boot_disk_type':'pd-standard',
            'boot_disk_size_gb':30
        }
    },
    'worker_config':{
        'num_instances':2,
        'machine_type_uri':'n1-standard-2',
        'disk_config':{
            'boot_disk_type':'pd-standard',
            'boot_disk_size_gb':30
        }
    },
    'software_config':{
        'image_version':'2.2-debian12'
    }
}

# sensor_task=GCSObjectExistenceSensor(
#     task_id="Sensing_file",
#     bucket="spark_ex_airflow",
#     object='assignment1/data/employee-{{ ti.xcom_pull(task_ids="Execution_date") }}.csv',
#     google_cloud_conn_id="google_cloud_default",
#     timeout=300,
#     poke_interval=300,
#     mode="poke",
#     dag=dag,
# )

def check_gcs_file(**kwargs):
    execution_date = kwargs['ti'].xcom_pull(task_ids='Execution_date')
    file_path = f"assignment1/data/employee-{execution_date}.csv"
    client = storage.Client()
    bucket = client.bucket("spark_ex_airflow")
    blob = bucket.blob(file_path)
    
    return blob.exists()

sensor_task = PythonSensor(
    task_id="Sensing_file",
    python_callable=check_gcs_file,
    poke_interval=30,
    timeout=300,
    mode="poke",
    #provide_context=True,
    dag=dag,
)

create_cluster=DataprocCreateClusterOperator(
    task_id="Creating_Dataproc_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag,
)

spark_task=DataprocSubmitJobOperator(
    task_id="Submit_Spark_Job",
    region=REGION,
    project_id=PROJECT_ID,
    job={
        "placement":{"cluster_name":CLUSTER_NAME},
        "pyspark_job":{
            "main_python_file_uri":"gs://spark_ex_airflow/assignment1/spark_job/spark_job.py",
            "args": ['--date={{ ti.xcom_pull(task_ids="Execution_date") }}']
        },
    },
    dag=dag,
)
delete_cluster=DataprocDeleteClusterOperator(
    task_id="deleting_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule="all_done",
    dag=dag,
)


python_get_execution_date>>sensor_task>>create_cluster>>spark_task>>delete_cluster
