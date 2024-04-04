import json
import vertica_python
import pendulum
import os
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

def get_execution_date(**context) -> str:
    execution_date = os.environ["AIRFLOW_CTX_EXECUTION_DATE"]
    return str(execution_date).split('T')[0]

def load_global_metrics_dwh(conn_info=vertica_conn_settings):
    date = get_execution_date()
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                insert into stv2023111354__dwh.global_metrics
                with c as (
                select
                    *
                from
                    stv2023111354__staging.currencies c
                where
                    currency_code_with = 420
                    and date_update = '{date}'::datetime
                ),
                t as (
                select
                    c.date_update,
                    t.currency_code as currency_from,
                    t.account_number_from,
                    (t.amount * c.currency_with_div) as amount
                from
                    stv2023111354__staging.transactions t
                join c
                on
                    t.transaction_dt::date = c.date_update
                    and t.currency_code = c.currency_code
                where
                    t.status = 'done'
                    and t.account_number_from>0
                    and t.transaction_dt::date = '{date}'::datetime
                union all
                select
                    transaction_dt::date as date_update,
                    currency_code as currency_from,
                    account_number_from,
                    amount
                from
                   stv2023111354__staging.transactions
                where
                    currency_code = 420
                    and status = 'done'
                    and account_number_from>0
                    and transaction_dt::date = '{date}'::datetime
                )
                select
                    date_update,
                    currency_from,
                    sum(amount) as amount_total,
                    count(*) as cnt_transactions,
                    round(sum(amount)/ count(distinct account_number_from), 2) as avg_transactions_per_account,
                    count(distinct account_number_from) as cnt_accounts_make_transactions
                from
                    t
                group by
                    date_update,
                    currency_from
                ;
                """
            )
            res = cur.fetchall()
        except vertica_python.errors.Error as e:
            raise Exception(f"Error to update global metric: {str(e)}")
        return res


@dag(
    schedule_interval="0 12 * * *",
    start_date=pendulum.parse("2022-10-01"),
    catchup=True,
)
def dwh_dag3():
    load_global_metrics_task = PythonOperator(
        task_id="load_global_metrics",
        python_callable=load_global_metrics_dwh
    )

    load_global_metrics_task


_ = dwh_dag3()