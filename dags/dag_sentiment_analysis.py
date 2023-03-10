# Import libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


# Instance DAG object
with DAG(
    'sentiment_analysis',
    default_args={
        'depends_on_past':False,
        'retries':2,
        'retry_delay':timedelta(seconds=30),
    },
    description='News articles sentiment analysis.',
	schedule_interval='*/3 * * * * *',
    start_date=datetime(2023,3,1),
    catchup=False,
) as dag:
    
    # Task #1: Get news articles
    task1 = BashOperator(
        task_id='get_news',
        bash_command='python3 news.py',
        retries=1,
    )
    
    # Task #2: Perform sentiment analysis on news articles
    task2 = BashOperator(
        task_id='sentiment_analysis',
        bash_command='python3 sentiment.py',
        retries=1,
        depends_on_past=True,
    )
    
    # Task precedency
    task1 >> task2