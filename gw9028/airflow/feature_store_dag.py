from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator, PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fs_dag',
    default_args=default_args,
    description='Sacombank Feature Store DAG',
    schedule_interval='@daily',
    catchup=False,
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# BEGIN: 5j3d8f4g7h9k

def f_customer():
    # Your code to process customer data goes here
    pass

def f_card_dim():
    # Your code to process card dimension data goes here
    pass

def f_eb_mb_crossell():
    # Your code to process eb_mb_crossell data goes here
    pass

customer = PythonOperator(
    task_id='customer',
    python_callable=f_customer,
    dag=dag
)

eb_mb_crossell = PythonOperator(
    task_id='eb_mb_crossell',
    python_callable=f_eb_mb_crossell,
    dag=dag
)

card_dim = PythonOperator(
    task_id='card_dim',
    python_callable=f_card_dim,
    dag=dag
)

customer >> eb_mb_crossell
card_dim >> end

# END: 5j3d8f4g7h9k

start >> customer

CINS_TMP_CUST
CINS_TMP_CARD_DIM
CINS_TMP_EB_MB_CROSSELL
CINS_TMP_CREDIT_CARD_TRANSACTION
CINS_TMP_CUSTOMER_STATUS
CINS_TMP_DATA_RPT_CARD_{RPT_DT}
CINS_TMP_DATA_RPT_LOAN_{RPT_DT}