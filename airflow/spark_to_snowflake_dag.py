from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

SNOWSQL_CONN = "snowsql -a <ACCOUNT> -u <USER> -d JOB_ANALYTICS -s ANALYTICS -w COMPUTE_WH"

SPARK_OUTPUT_BASE = "/home/snehal_thorat/Job Analytics/spark/output"

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="spark_to_snowflake_job_analytics",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,   # manual trigger
    catchup=False,
    default_args=default_args,
) as dag:

    start = EmptyOperator(task_id="start")

    # ----------------------------
    # PUT FILES TO SNOWFLAKE STAGE
    # ----------------------------

    put_dim_company = BashOperator(
        task_id="put_dim_company",
        bash_command=f"""
        {SNOWSQL_CONN} -q "
        PUT 'file://{SPARK_OUTPUT_BASE}/dim_company/*.snappy.parquet'
        @JOB_ANALYTICS_STAGE/dim_company
        AUTO_COMPRESS=FALSE;
        "
        """
    )

    put_dim_job = BashOperator(
        task_id="put_dim_job",
        bash_command=f"""
        {SNOWSQL_CONN} -q "
        PUT 'file://{SPARK_OUTPUT_BASE}/dim_job/*.snappy.parquet'
        @JOB_ANALYTICS_STAGE/dim_job
        AUTO_COMPRESS=FALSE;
        "
        """
    )

    put_dim_applicant = BashOperator(
        task_id="put_dim_applicant",
        bash_command=f"""
        {SNOWSQL_CONN} -q "
        PUT 'file://{SPARK_OUTPUT_BASE}/dim_applicant/*.snappy.parquet'
        @JOB_ANALYTICS_STAGE/dim_applicant
        AUTO_COMPRESS=FALSE;
        "
        """
    )

    put_dim_region = BashOperator(
        task_id="put_dim_region",
        bash_command=f"""
        {SNOWSQL_CONN} -q "
        PUT 'file://{SPARK_OUTPUT_BASE}/dim_region/*.snappy.parquet'
        @JOB_ANALYTICS_STAGE/dim_region
        AUTO_COMPRESS=FALSE;
        "
        """
    )

    put_fact = BashOperator(
        task_id="put_fact",
        bash_command=f"""
        {SNOWSQL_CONN} -q "
        PUT 'file://{SPARK_OUTPUT_BASE}/fact_job_applications/*.snappy.parquet'
        @JOB_ANALYTICS_STAGE/fact_job_applications
        AUTO_COMPRESS=FALSE;
        "
        """
    )

    # ----------------------------
    # COPY INTO STAGING TABLES
    # ----------------------------

    copy_dim_company = BashOperator(
        task_id="copy_dim_company",
        bash_command=f"""
        {SNOWSQL_CONN} -q "
        COPY INTO STG_DIM_COMPANY
        FROM @JOB_ANALYTICS_STAGE/dim_company
        FILE_FORMAT=(TYPE=PARQUET)
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        FORCE=TRUE;
        "
        """
    )

    copy_dim_job = BashOperator(
        task_id="copy_dim_job",
        bash_command=f"""
        {SNOWSQL_CONN} -q "
        COPY INTO STG_DIM_JOB
        FROM @JOB_ANALYTICS_STAGE/dim_job
        FILE_FORMAT=(TYPE=PARQUET)
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        FORCE=TRUE;
        "
        """
    )

    copy_dim_applicant = BashOperator(
        task_id="copy_dim_applicant",
        bash_command=f"""
        {SNOWSQL_CONN} -q "
        COPY INTO STG_DIM_APPLICANT
        FROM @JOB_ANALYTICS_STAGE/dim_applicant
        FILE_FORMAT=(TYPE=PARQUET)
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        FORCE=TRUE;
        "
        """
    )

    copy_dim_region = BashOperator(
        task_id="copy_dim_region",
        bash_command=f"""
        {SNOWSQL_CONN} -q "
        COPY INTO STG_DIM_REGION
        FROM @JOB_ANALYTICS_STAGE/dim_region
        FILE_FORMAT=(TYPE=PARQUET)
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        FORCE=TRUE;
        "
        """
    )

    copy_fact = BashOperator(
        task_id="copy_fact",
        bash_command=f"""
        {SNOWSQL_CONN} -q "
        COPY INTO STG_FACT_JOB_APPLICATIONS
        FROM @JOB_ANALYTICS_STAGE/fact_job_applications
        FILE_FORMAT=(TYPE=PARQUET)
        MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        FORCE=TRUE;
        "
        """
    )

    end = EmptyOperator(task_id="end")

    # ----------------------------
    # DAG DEPENDENCIES
    # ----------------------------

    start >> [
        put_dim_company,
        put_dim_job,
        put_dim_applicant,
        put_dim_region,
        put_fact
    ]

    put_dim_company >> copy_dim_company
    put_dim_job >> copy_dim_job
    put_dim_applicant >> copy_dim_applicant
    put_dim_region >> copy_dim_region
    put_fact >> copy_fact

    [
        copy_dim_company,
        copy_dim_job,
        copy_dim_applicant,
        copy_dim_region,
        copy_fact
    ] >> end
