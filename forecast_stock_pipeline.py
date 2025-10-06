from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SNOWFLAKE_TRAINING_INPUT_TABLE = Variable.get("snowflake_stock_table")
SNOWFLAKE_TRAINING_INPUT_VIEW = Variable.get("snowflake_stock_table_view")
SNOWFLAKE_FORECAST_FUNCTION_NAME = Variable.get(
    "snowflake_forecast_function_name")
SNOWFLAKE_FORECAST_TABLE = Variable.get("snowflake_forecast_table")
SNOWFLAKE_FINAL_TABLE = Variable.get("snowflake_final_table")


def get_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def train(cursor):
    create_view_sql = f"""CREATE OR REPLACE VIEW {SNOWFLAKE_TRAINING_INPUT_VIEW} AS SELECT
        to_timestamp_ntz(date) as date_timestamp,
        close,
        symbol
        FROM {SNOWFLAKE_TRAINING_INPUT_TABLE};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {SNOWFLAKE_FORECAST_FUNCTION_NAME} (
        INPUT_DATA => TABLE({SNOWFLAKE_TRAINING_INPUT_VIEW}),
        SERIES_COLNAME => 'symbol',
        TIMESTAMP_COLNAME => 'date_timestamp',
        TARGET_COLNAME => 'close',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cursor.execute(create_view_sql)
        cursor.execute(create_model_sql)
        cursor.execute(
            f"CALL {SNOWFLAKE_FORECAST_FUNCTION_NAME}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        cursor.execute('ROLLBACK;')
        print(e)
        raise


@task
def predict(cursor):
    make_prediction_sql = f"""
        CREATE OR REPLACE TABLE {SNOWFLAKE_FORECAST_TABLE} AS 
        SELECT * FROM TABLE({SNOWFLAKE_FORECAST_FUNCTION_NAME}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        ));
    """

    create_final_table_sql = f"""
        CREATE OR REPLACE TABLE {SNOWFLAKE_FINAL_TABLE} AS
        SELECT symbol, to_timestamp_ntz(date) as date, close AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {SNOWFLAKE_TRAINING_INPUT_TABLE}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {SNOWFLAKE_FORECAST_TABLE};"""

    try:
        cursor.execute(make_prediction_sql)
        cursor.execute(create_final_table_sql)
    except Exception as e:
        cursor.execute('ROLLBACK;')
        print(e)
        raise


with DAG(
    dag_id='forecast_stock_data',
    catchup=False,
    start_date=datetime(2025, 9, 29),
    tags=['ML', 'ELT']
) as dag:
    cursor = get_snowflake_conn()

    train_task = train(cursor)
    predict_task = predict(cursor)

    train_task >> predict_task
