import sys
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from custom_modules.main import main
from airflow.models import Variable


sys.path.append(
    "~\\airflow_set_up\\dags\\custom_modules")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
    'data_interval_start': datetime(2023, 11, 17, 9, 0, 0),
    'data_interval_end': datetime(2023, 11, 17, 13, 0, 0),
    'schedule': timedelta(days=4),

}

dag = DAG(
    'test',
    default_args=default_args,
    description='We are testing..',
    schedule_interval=timedelta(days=1),
)


# db variables
db_name = Variable.get("PG_DBNAME")
db_host = Variable.get("PG_HOST")
db_password = Variable.get("PG_PW")
db_user = Variable.get("PG_USER")
s3_bucket_name = Variable.get("S3_BUCKET_NAME")
aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("AWS_ACCESS_KEY")
slack_url = Variable.get("SLACK_URL")

task1 = PythonOperator(
    task_id='dag_test',
    python_callable=main,
    op_kwargs={"db_host": db_host,
               "db_name": db_name, "db_pw": db_password,
               "db_user": db_user, "bucket_name": s3_bucket_name,
               "aws_access_key_id": aws_access_key_id, "aws_secret_access_key": aws_secret_access_key,
               "slack_url": slack_url},
    dag=dag,
)

task1
