from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
import sql_statements

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'kaguara',
    #'start_date': datetime(2019, 1, 12),
    'start_date': datetime(2019, 3, 8),
}

dag = DAG('kaguara_capstone_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1
          #schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

################ START DELETE TABLES OPERATORS ###############
delete_staging_transactions_table = PostgresOperator(
    task_id="delete_staging_transactions_table",
    dag=dag,
    sql=sql_statements.staging_transactions_table_drop_sql,
    postgres_conn_id="redshift"
)

delete_merchants_table = PostgresOperator(
    task_id="delete_merchants_table",
    dag=dag,
    sql=sql_statements.merchants_table_drop_sql,
    postgres_conn_id="redshift"
)

delete_customers_table = PostgresOperator(
    task_id="delete_customers_table",
    dag=dag,
    sql=sql_statements.customers_table_drop_sql,
    postgres_conn_id="redshift"
)

################ END DELETE TABLES OPERATORS ###############

################ START CREATE TABLES OPERATORS ###############
create_staging_transactions_table = PostgresOperator(
    task_id="create_staging_transactions_table",
    dag=dag,
    sql=sql_statements.CREATE_STAGING_TRANSACTIONS_TABLE_SQL,
    postgres_conn_id="redshift"
)

create_merchants_table = PostgresOperator(
    task_id="create_merchants_table",
    dag=dag,
    sql=sql_statements.CREATE_MERCHANTS_TABLE_SQL,
    postgres_conn_id="redshift"
)

create_customers_table = PostgresOperator(
    task_id="create_customers_table",
    dag=dag,
    sql=sql_statements.CREATE_CUSTOMERS_TABLE_SQL,
    postgres_conn_id="redshift"
)
################ END CREATE TABLES OPERATORS ###############

stage_transactions_to_redshift = StageToRedshiftOperator(
    task_id='stage_transactions',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_transactions",
    s3_bucket="udacity-capstone-kaguara-source-bucket",
    s3_key="PS_20174392719_1491204439457_log.csv",
    ignore_headers=1,
    delimiter=","
)

load_merchants_dimension_table = LoadDimensionOperator(
    task_id='load_merchants_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="merchants",
    insert_query = sql_statements.INSERT_INTO_MERCHANTS_TABLE_SQL
)

load_customers_dimension_table = LoadDimensionOperator(
    task_id='load_customers_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="merchants",
    insert_query = sql_statements.INSERT_INTO_CUSTOMERS_TABLE_SQL
)

#run_quality_checks = DataQualityOperator(
#    task_id='Run_data_quality_checks',
#    dag=dag
#)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> delete_staging_transactions_table
start_operator >> delete_merchants_table
start_operator >> delete_customers_table

delete_staging_transactions_table >> create_staging_transactions_table
delete_merchants_table >> create_merchants_table
delete_customers_table >> create_customers_table

create_staging_transactions_table >> stage_transactions_to_redshift
create_merchants_table >> stage_transactions_to_redshift
create_customers_table >> stage_transactions_to_redshift


stage_transactions_to_redshift >> load_merchants_dimension_table

load_merchants_dimension_table >> load_customers_dimension_table

load_customers_dimension_table >> end_operator


#run_quality_checks >> end_operator