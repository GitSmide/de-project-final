import json
import boto3
import vertica_python
import pendulum
#import conn_settings as c

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# это будет спрятано в отдельном файле, для отладки пришлось вынести
vertica_conn_settings = {
    'host': 'vertica.tgcloudenv.ru',
    'port': '5433',
    'user': 'stv2023111354',
    'password': '0vCavQfqudvXFOC',
    "autocommit": True
}
# это будет спрятано в отдельном файле, для отладки пришлось вынести
vertica_access_settings = {
    'AWS_ACCESS_KEY_ID': 'YCAJEWXOyY8Bmyk2eJL-hlt2K',
    'AWS_SECRET_ACCESS_KEY': 'YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA'
}

def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id= vertica_access_settings['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key= vertica_access_settings['AWS_SECRET_ACCESS_KEY'],
    ) 

    s3_client.download_file(Bucket=bucket, Key=key, Filename=f"../../../data/{key}")


def load_currencies_staging(conn_info=vertica_conn_settings):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """copy stv2023111354__staging.currencies
                (currency_code,currency_code_with,date_update,currency_with_div)
                from local"""
                "'/data/currencies_history.csv' delimiter ','"
                "rejected data '/data/currencies_rejects.txt'"
                "exceptions '/data/currencies_exceptions.txt'",
                buffer_size=65536,
            )
            res = cur.fetchall()
        except vertica_python.errors.Error as er:
            raise Exception(f"Error to currencies data load: {str(er)}")
        return res


def load_transactions_staging(file_num, conn_info=vertica_conn_settings):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """copy stv2023111354__staging.transactions
                (operation_id,account_number_from,account_number_to,currency_code,
                country,status,transaction_type,amount,transaction_dt)
                from local"""
                f"'/data/transactions_batch_{file_num}.csv' delimiter ','"
                f"rejected data '/data/transactions_batch_{file_num}_rejects.txt'"
                f"exceptions '/data/transactions_batch_{file_num}_exceptions.txt'",
                buffer_size=65536,
            )
            res = cur.fetchall()
        except vertica_python.errors.Error as er:
            raise Exception(
                f"Error to transactions data load: {str(er)}"
            )
        return res


@dag(
    schedule_interval="0 12 1 * *",
    start_date=pendulum.parse("2022-10-01"),
    catchup=False,
)
def staging_dag():
    fetch_currencies_task = PythonOperator(
        task_id="fetch_currencies",
        python_callable=fetch_s3_file,
        op_kwargs={"bucket": "final-project", "key": "currencies_history.csv"},
    )

    load_currencies_task = PythonOperator(
        task_id="load_currencies_staging",
        python_callable=load_currencies_staging,
    )

    fetch_transactions_task = []
    load_transactions_task = []
    for i in range(1, 11):
        fetch_transactions_task.append(
            PythonOperator(
                task_id=f"fetch_transactions_{i}",
                python_callable=fetch_s3_file,
                op_kwargs={
                    "bucket": "final-project",
                    "key": f"transactions_batch_{i}.csv",
                },
            )
        )

        load_transactions_task.append(
            PythonOperator(
                task_id=f"load_transactions_staging_{i}",
                python_callable=load_transactions_staging,
                op_kwargs={"file_num": i},
            )
        )

    (
        fetch_currencies_task
        >> fetch_transactions_task
        >> load_currencies_task
        >> load_transactions_task
    )

_ = staging_dag()