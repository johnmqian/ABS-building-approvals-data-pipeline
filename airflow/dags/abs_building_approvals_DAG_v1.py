# dags/abs_building_approvals_pipeline.py

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# ── Pipeline constants ────────────────────────────────────────────────────────
S3_BUCKET          = "johnq-data-lake-dev"
S3_PROJECT_PREFIX  = "abs-building-approvals"
AWS_REGION         = "ap-southeast-2"
TZ                 = pendulum.timezone("Australia/Melbourne")

default_args = {
    "owner":        "airflow",
    "retries":      2,
    "retry_delay":  timedelta(minutes=1),
}

# ── Param generator ───────────────────────────────────────────────────────────
def generate_params(**context) -> None:
    
    # Single source of truth for ingestion_date and run_id generated at runtime
    # logical_date is the UTC timestamp Airflow freezes when the run is created
    # (equivalent to data_interval_start for scheduled runs, trigger time for
    # manual runs). Converting it to Melbourne time here — once — means Bronze,
    #Silver, and Gold all see the same date even if the run crosses midnight.
    
    ingestion_date = (
        context["logical_date"]          # pendulum DateTime, UTC
        .in_timezone(TZ)                 # shift to Melbourne
        .strftime("%Y-%m-%d")            # "2026-03-16"
    )
    run_id = context["run_id"]           # Airflows built-in unique ID for the run e.g. "manual__2026-03-16T02:00:00+00:00"

    # Stores both values in Airflow's XCom, which is for values computed at runtime
    # XCOM - Cross communication is Airflow's built-in system for passing small values between tasks. When a task calls .xcom_push(key, value), Airflow stores that value in its metadata database. Any downstream task can then call .xcom_pull(task_ids, key) to retrieve it.
    context["ti"].xcom_push(key="ingestion_date", value=ingestion_date) 
    context["ti"].xcom_push(key="run_id",         value=run_id)


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id      = "abs_building_approvals_pipeline_DAG_v2",
    default_args = default_args,
    start_date  = pendulum.datetime(2026, 1, 1, tz=TZ),
    schedule    = None,
    catchup     = False,
    # Parameters known before the run as opposed to the XCom values generated at runtime. This block triggers a form in the Airflow UI
    params = {
        "start_period": "2021-07",
        "end_period": "2026-01",
    },
) as dag:

    # 1. Generate stable params
    generate_params_task = PythonOperator(
        task_id         = "generate_params",
        python_callable = generate_params,
    )

    # 2. Bronze — params injected as env vars

    # Runs as a separate OS process via BashOperator (Airflow launches: bash -> python script), rather than executing inside an Airflow @task / PythonOperator.
    # We keep Bronze as a standalone “job” because it’s larger and easier to run/debug outside Airflow, and it avoids loading heavy ETL dependencies (packages) into the Airflow task runner.
    # Therefore  the script reads config via os.getenv(...), and we inject run-specific variables here
    # (especially INGESTION_DATE) using env=..., while append_env=True preserves container-level creds.
    run_bronze_ingestion = BashOperator(
        task_id      = "run_bronze_extraction_ingestion",
        bash_command = "set -euo pipefail; python /opt/project/ingestion/ABS_Building_Approvals_Extraction.py",
        env = {
            "PYTHONPATH":        "/opt/project",
            "S3_BUCKET":         S3_BUCKET,
            "S3_PROJECT_PREFIX": S3_PROJECT_PREFIX,
            "AWS_REGION":        AWS_REGION,
            "START_PERIOD":      "{{ params.start_period }}",
            "END_PERIOD":        "{{ params.end_period }}",
            "INGESTION_DATE":    "{{ ti.xcom_pull(task_ids='generate_params', key='ingestion_date') }}",
            "RUN_ID":            "{{ ti.xcom_pull(task_ids='generate_params', key='run_id') }}",
        },
        append_env = True,
    )

    # 3. Silver — params via notebook_params
    run_silver_databricks = DatabricksRunNowOperator(
        task_id           = "run_silver_databricks",
        databricks_conn_id = "databricks_default",
        job_id            = 35620647428943,
        notebook_params   = {
            "ingestion_date": "{{ ti.xcom_pull(task_ids='generate_params', key='ingestion_date') }}",
            "run_id":         "{{ ti.xcom_pull(task_ids='generate_params', key='run_id') }}",
        },
    )

    # 4. Gold — same pattern as Silver
    run_gold_databricks = DatabricksRunNowOperator(
        task_id           = "run_gold_databricks",
        databricks_conn_id = "databricks_default",
        job_id            = 114657385781599,
        notebook_params   = {
            "ingestion_date": "{{ ti.xcom_pull(task_ids='generate_params', key='ingestion_date') }}",
            "run_id":         "{{ ti.xcom_pull(task_ids='generate_params', key='run_id') }}",
        },
    )

    # 5. Task chain
    generate_params_task >> run_bronze_ingestion >> run_silver_databricks >> run_gold_databricks