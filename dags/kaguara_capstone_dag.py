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
    'owner': 'udacity',
    #'start_date': datetime(2019, 1, 12),
    'start_date': datetime(2019, 3, 8),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1
          #schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

################ START DELETE TABLES OPERATORS ###############
delete_staging_events_table = PostgresOperator(
    task_id="delete_staging_events_table",
    dag=dag,
    sql=sql_statements.staging_events_table_drop_sql,
    postgres_conn_id="redshift"
)

delete_staging_songs_table = PostgresOperator(
    task_id="delete_staging_songs_table",
    dag=dag,
    sql=sql_statements.staging_songs_table_drop_sql,
    postgres_conn_id="redshift"
)
################ END DELETE TABLES OPERATORS ###############

################ START CREATE TABLES OPERATORS ###############
create_staging_events_table = PostgresOperator(
    task_id="create_staging_events_table",
    dag=dag,
    sql=sql_statements.CREATE_STAGING_EVENTS_TABLE_SQL,
    postgres_conn_id="redshift"
)

create_staging_songs_table = PostgresOperator(
    task_id="create_staging_songs_table",
    dag=dag,
    sql=sql_statements.CREATE_STAGING_SONGS_TABLE_SQL,
    postgres_conn_id="redshift"
)
################ END CREATE TABLES OPERATORS ###############

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11",
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A/",
    json="auto"
)

#load_songplays_table = LoadFactOperator(
#    task_id='Load_songplays_fact_table',
#    dag=dag
#)

#load_user_dimension_table = LoadDimensionOperator(
#    task_id='Load_user_dim_table',
#    dag=dag
#)

#load_song_dimension_table = LoadDimensionOperator(
#    task_id='Load_song_dim_table',
#    dag=dag
#)

#load_artist_dimension_table = LoadDimensionOperator(
#    task_id='Load_artist_dim_table',
#    dag=dag
#)

#load_time_dimension_table = LoadDimensionOperator(
#    task_id='Load_time_dim_table',
#    dag=dag
#)

#run_quality_checks = DataQualityOperator(
#    task_id='Run_data_quality_checks',
#    dag=dag
#)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> delete_staging_events_table
start_operator >> delete_staging_songs_table

delete_staging_events_table >> create_staging_events_table
delete_staging_songs_table >> create_staging_songs_table
create_staging_events_table >> stage_events_to_redshift
create_staging_songs_table >> stage_events_to_redshift
stage_events_to_redshift >> stage_songs_to_redshift
#stage_songs_to_redshift >> load_songplays_table
#load_songplays_table >> load_user_dimension_table
#load_songplays_table >> load_song_dimension_table
#load_songplays_table >> load_artist_dimension_table
#load_songplays_table >> load_time_dimension_table
#load_user_dimension_table >> run_quality_checks
#load_song_dimension_table >> run_quality_checks
#load_artist_dimension_table >> run_quality_checks
#load_time_dimension_table >> run_quality_checks
#run_quality_checks >> end_operator

stage_songs_to_redshift >> end_operator