import logging
import snowflake.connector
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow import AirflowException
from airflow.utils.email import send_email_smtp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner' : 'success',
    'depends_on_past': False,
    'start_date': datetime(2020, 3, 17),
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 2)
}

# Snowflake information
# Information must be stored in connections
# It can be done with Airflow UI -
# Admin -> Connections -> Create
database_name = 'TEST_DB'
schema_name = 'public'
snowflake_username = BaseHook.get_connection('snowflake_conn').login
snowflake_password = BaseHook.get_connection('snowflake_conn').password
snowflake_account = BaseHook.get_connection('snowflake_conn').host

dag = DAG(
    dag_id="load_delete_done",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
)
def notify_email(contextDict, **kwargs):
    """Send custom email alerts."""

    # email title.
    title = "Airflow alert: {task_name} Failed".format(**contextDict)

    # email contents
    body = """
    Hi Everyone, <br>
    <br>
    There's been an error in the {task_name} job.<br>
    <br>
    Forever yours,<br>
    Airflow bot <br>
    """.format(**contextDict)

    send_email_smtp('tapas.das@datagrokr.com', title, body)


def upload_to_snowflake():
    con = snowflake.connector.connect(user=snowflake_username, password=snowflake_password, account=snowflake_account)
    cs = con.cursor()
    load_data = "call upload_customer();"

    try:
        status = cs.execute().fetchall()[0][0]
        if status == 'Done.':
            logger.info("Procedure executed successfully")
        else:
            logger.error("Procedure failed with {}".format(str(status)))
            raise AirflowException("Failed procedure run.")
    except Exception as ex:
        logger.error("Procedure failed with {}".format(str(ex)))
        raise AirflowException("Failed procedure run.")
    finally:
        cs.close()

def delete_to_snowflake():
    con = snowflake.connector.connect(user=snowflake_username, password=snowflake_password, account=snowflake_account)
    cs = con.cursor()
    delete_data = "call drop_customer();"

    try:
        status = cs.execute(load_sql).fetchall()[0][0]
        if status == 'Done.':
            logger.info("Procedure executed successfully")
        else:
            logger.error("Procedure failed with {}".format(str(status)))
            raise AirflowException("Failed procedure run.")
    except Exception as ex:
        logger.error("Procedure failed with {}".format(str(ex)))
        raise AirflowException("Failed procedure run.")
    finally:
        cs.close()

with dag:
    load_table = SnowflakeOperator(
        task_id = 'snowflake_load',
        snowflake_conn_id = "snowflake_allokay"
    )

with dag:
    delete_table = SnowflakeOperator(
        task_id = 'snowflake_delete',
        snowflake_conn_id = "snowflake_allokay"
    )
load_table >> drop_table