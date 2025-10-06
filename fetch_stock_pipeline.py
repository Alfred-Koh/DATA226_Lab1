from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

STOCKS = Variable.get("stock_tickers", deserialize_json=True)
SNOWFLAKE_TABLE = Variable.get("snowflake_stock_table")
HISTORY_PERIOD = Variable.get("stock_history_period", default_var="180d")


@task
def fetch_stock_data_task():
    data = []
    for ticker in STOCKS:
        stock = yf.Ticker(ticker)
        hist = stock.history(period=HISTORY_PERIOD)
        hist.reset_index(inplace=True)
        hist['symbol'] = ticker
        hist.rename(columns={
            'Open': 'open',
            'Close': 'close',
            'Low': 'min',
            'High': 'max',
            'Volume': 'volume',
            'Date': 'date'
        }, inplace=True)
        hist['date'] = hist['date'].dt.strftime('%Y-%m-%d')
        data.extend(hist.to_dict(orient='records'))

    return data


@task
def transform_stock_data_task(data):
    df = pd.DataFrame(data)
    df[['open', 'close', 'min', 'max']] = df[[
        'open', 'close', 'min', 'max']].round(2)
    dict_records = df.to_dict(orient='records')

    return dict_records


@task
def insert_into_snowflake_task(data):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("BEGIN;")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
                symbol STRING,
                date DATE,
                open FLOAT,
                close FLOAT,
                min FLOAT,
                max FLOAT,
                volume INT,
                PRIMARY KEY (symbol, date)
            );
        """)
        cursor.execute(f'DELETE FROM {SNOWFLAKE_TABLE};')

        for day in data:
            symbol = day['symbol']
            date = day['date']
            open = day["open"]
            close = day["close"]
            min = day["min"]
            max = day["max"]
            volume = day["volume"]
            cursor.execute(
                f"INSERT INTO {SNOWFLAKE_TABLE} (symbol, date, open, min, max, close, volume) VALUES ('{symbol}', '{date}', {open}, {min}, {max}, {close}, {volume})")

        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute('ROLLBACK;')
        print(e)
        raise e

    cursor.close()
    conn.close()


with DAG(
    'fetch_stock_data',
    default_args=default_args,
    description='Fetch last 180 days stock data and load to Snowflake',
    schedule_interval='0 1 * * *',
    start_date=datetime(2025, 9, 29),
    catchup=False,
    tags=['ML', 'ETL']
) as dag:
    data = fetch_stock_data_task()
    transformed = transform_stock_data_task(data)
    load_task = insert_into_snowflake_task(transformed)

    trigger_forecast = TriggerDagRunOperator(
        task_id="trigger_forecast_pipeline",
        trigger_dag_id="forecast_stock_data",
        wait_for_completion=False,
        reset_dag_run=True,
        poke_interval=60,
    )

    load_task >> trigger_forecast
