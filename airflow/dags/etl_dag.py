from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


# source: https://airflow.apache.org/docs/stable/tutorial.html
default_args = {
    'owner': 'ucaiado',
    'start_date': datetime(2020, 5, 25),
    # The DAG does not have dependencies on past runs
    'depends_on_past': False,
    # On failure, the task are retried 3 times
    'retries': 3,
    # Retries happen every 5 minutes
    'retry_delay': timedelta(minutes=5),
    # Catchup is turned off
    'catchup': False,
    # Do not email on retry
    'email_on_retry': False

}

dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id='Create_tables',
    sql='create_tables.sql',
    postgres_conn_id='redshift',
    dag=dag
    )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    postgres_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_path=Variable.get('S3_LOG_DATA'),
    json_data=Variable.get('S3_LOG_JSONPATH'),
    dag=dag
    )

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    postgres_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_path=Variable.get('S3_SONG_DATA'),
    json_data="'auto'",
    dag=dag
    )

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    postgres_conn_id='redshift',
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert,
    dag=dag
    )

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    postgres_conn_id='redshift',
    table='users',
    sql_query=SqlQueries.user_table_insert,
    delete_load=True,
    dag=dag
    )

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    postgres_conn_id='redshift',
    table='songs',
    sql_query=SqlQueries.song_table_insert,
    delete_load=True,
    dag=dag
    )

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    postgres_conn_id='redshift',
    table='artists',
    sql_query=SqlQueries.artist_table_insert,
    delete_load=True,
    dag=dag
    )

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    postgres_conn_id='redshift',
    table='time',
    sql_query=SqlQueries.time_table_insert,
    delete_load=True,
    dag=dag
    )

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    postgres_conn_id='redshift',
    tables=['staging_events', 'staging_songs', 'songplays', 'users', 'songs',
            'artists', 'time'],
    dag=dag
    )

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


# DAGs dependencies
# source: https://www.astronomer.io/guides/managing-dependencies/

start_operator >> create_tables
create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
[load_user_dimension_table, load_song_dimension_table] >> run_quality_checks
[load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator
