from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime,timedelta
import csv
import requests
import json
# These arguments are applied to Tasks not DAG
default_args = {
    "owner":"airflow",
    "email_on_failure":False,
    "email_on_retry":False,
    "email":"admin@localhost.com",
    "retries": 3,
    "retry_delay":timedelta(minutes=5)
}

def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')
    
def _get_message() -> str:
    return (
        ":tada: :confetti_ball: *Forex Airflow Data Pipeline Execution Completed Successfully!* :confetti_ball: :tada:\n"
        "> :heavy_check_mark: All tasks finished successfully without errors.\n"
        "> :chart_with_upwards_trend: The latest forex data has been processed and is now up-to-date.\n"
        "> :rocket: Great job team! Keep up the good work!\n"
        " \n"
    )

with DAG("forex_data_pipeline2", start_date = datetime(2024,1,1), schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    is_forex_rates_available = HttpSensor(
        task_id = "Are-Forex-Rates-Available",
        http_conn_id = "forex_api2", 
        endpoint = "marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check = lambda response: "rates" in response.text,
        poke_interval = 5,
        timeout = 20,
    )

    is_forex_currencies_available = FileSensor(
        task_id = "Is-Currency-CSV-File-Available",
        fs_conn_id = "forex_path2",
        filepath = "forex_currencies.csv",
        poke_interval = 5,
        timeout = 20,
    )

    download_rates_operator = PythonOperator(
        task_id = "Download-Forex-Rates-Python-Operator",
        python_callable = download_rates,

    )

    bash_command = """
    hdfs dfs -mkdir -p /forex && \
    hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
    """

    run_bash_and_save_to_hdfs = BashOperator(
        task_id='Bash-To-Save-Data-In-HDFS',
        bash_command=bash_command,
    )

    creating_forex_rates_table = HiveOperator(
    task_id="Create-Forex-Tables-Hive",
    hive_cli_conn_id="hive_conn",
    hql="""
        CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
            base STRING,
            last_update DATE,
            eur DOUBLE,
            usd DOUBLE,
            nzd DOUBLE,
            gbp DOUBLE,
            jpy DOUBLE,
            cad DOUBLE
            )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
    """
    )

    forex_processing = SparkSubmitOperator(
        task_id="Forex-Data-Processing-In-Spark",
        application="/opt/airflow/dags/scripts/forex_processing.py",
        conn_id="spark_conn",
        verbose=False
    )

    send_slack_notification = SlackWebhookOperator(
        task_id = "Notify-In-Slack",
        http_conn_id = "slack_conn",
        message=_get_message(),
        channel = '#monitoring'
    )

    is_forex_rates_available >> is_forex_currencies_available >> download_rates_operator >> run_bash_and_save_to_hdfs >> creating_forex_rates_table >> forex_processing >> send_slack_notification