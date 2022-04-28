from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='ElasticsearchConsumer',
    default_args=args,
    schedule_interval=None,
    tags=['ElasticsearchConsumer']
)



ElasticsearchConsumer = BashOperator(
    task_id='ElasticsearchConsumer',
    bash_command='python /home/naya/tmp/pycharm_project_137/ElasticsearchConsumer.py',
    dag=dag,
)


Start = DummyOperator(task_id='Start', retries=3, dag=dag)


ElasticsearchConsumer

if __name__ == "__main__":
    dag.cli()
