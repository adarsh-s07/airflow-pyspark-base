from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

def basic_load():
    spark = SparkSession.builder.appName('basic_load').getOrCreate()

    data_path = '/app/data'

    df = spark.read.csv(f'{data_path}/raw/authors.csv', inferSchema=True, header=True)

    df = df.withColumn("split_data", func.split(func.col("AUTHOR_NAME"), " "))

    df = df.select(
        func.col('AUTHOR_ID').alias('author_id'),
        func.col('split_data')[0].alias('first_name'),
        func.try_element_at(func.col('split_data'), func.lit(2)).alias('last_name')
    )

    df.write.csv(f'{data_path}/authors_cleaned.csv')


default_args = {
    "owner": "Toreid",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id = "python_sample_dag",
    description="second version of python dag",
    start_date=datetime(2026, 2, 1, 2),
    default_args=default_args,
    schedule="@daily"
) as dag:
    task1 = PythonOperator(
        task_id = "task1_import",
        python_callable=basic_load
    )

    task1