from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import LoadFactOperator,LoadDimensionOperator, DataQualityOperator
from operators.stage_redshift import StageToRedshiftOperator
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Dilorom',
    'start_date': datetime(2021, 1, 11),
    'end_date': datetime(2021, 1, 11),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('airflow_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '0 * * * *' # @hourly: Run once an hour at the beginning of the hour
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id = "create_tables",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = "create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    table = 'staging_events', 
    provide_context = True, 
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    file_format = 'JSON'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    table = 'staging_songs', 
    provide_context = True, 
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data',
    file_format = 'JSON'
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    provide_context = True,
    redshift_conn_id = 'redshift',
    table = 'songplays',
    column_list = ['playid', 'start_time', 'userid', 'level', 'songid', 'artistid', 'sessionid', 'location', 'user_agent'],
    sql_query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag, 
    provide_context = True,
    redshift_conn_id = 'redshift',
    table = 'users',
    column_list = ['userid', 'first_name', 'last_name', 'gender', 'level'],
    sql_query = SqlQueries.user_table_insert  
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag, 
    provide_context = True,
    redshift_conn_id = 'redshift',
    table = 'songs',
    column_list = ['songid', 'title', 'artistid', 'year', 'duration'],
    sql_query = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    provide_context =True,
    redshift_conn_id ='redshift',
    table = 'artists',
    column_list = ['artistid', 'name', 'location', 'lattitude', 'longitude'],
    sql_query = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag, 
    provide_context = True,
    redshift_conn_id ='redshift',
    table = 'time',
    column_list = ['start_time', 'hour', 'day', 'week', 'month','year','weekday'],
    sql_query = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id ='Run_data_quality_checks',
    dag = dag, 
    provide_context = True,
    redshift_conn_id ='redshift',
    tables = ['songplays','songs','artists','time','users'],
    sql_query = "SELECT COUNT(*) FROM {table}",
    expected_result = "greater than 0"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables >> stage_events_to_redshift >> load_songplays_table
create_tables >> stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator


