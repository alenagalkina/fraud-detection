from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "retry_delay": timedelta(minutes=5),
}


dag_spark = DAG(
    dag_id="spark_clean_data",
    default_args=default_args,
    schedule_interval="10 * * * *",
    dagrun_timeout=timedelta(minutes=10),
    start_date=datetime(2023, 7, 1),
)

s3_to_hdfs = SSHOperator(
    task_id="s3_to_hdfs",
    ssh_conn_id="ssh_new",
    command="hadoop distcp {}/raw/2022-10-05.txt {}user/ubuntu/".format(
        Variable.get("S3_BUCKET"), Variable.get("MASTER_NODE")
    ),
    cmd_timeout=3600,
    dag=dag_spark,
)

pyspark_job = SSHOperator(
    task_id="pyspark_job",
    ssh_conn_id="ssh_new",
    command="/usr/bin/spark-submit /home/ubuntu/scripts/spark_full.py",
    cmd_timeout=3600,
    dag=dag_spark,
)

hdfs_to_s3 = SSHOperator(
    task_id="hdfs_to_s3",
    ssh_conn_id="ssh_new",
    command="hadoop distcp -direct {}tmp/preprocessed/ {}".format(
        Variable.get("MASTER_NODE"), Variable.get("S3_BUCKET")
    ),
    cmd_timeout=3600,
    dag=dag_spark,
)

list_s3 = SSHOperator(
    task_id="list_s3",
    ssh_conn_id="ssh_new",
    command="hadoop fs -ls {}preprocessed".format(Variable.get("S3_BUCKET")),
    dag=dag_spark,
)

s3_to_hdfs >> pyspark_job >> hdfs_to_s3 >> list_s3

if __name__ == "__main__ ":
    dag_spark.cli()
