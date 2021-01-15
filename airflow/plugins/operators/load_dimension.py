from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 sql_query = '',
                 table = '',
                 column_list=[],
                 truncate = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.column_list= column_list
        self.truncate = truncate



    def execute(self, context):
        self.log.info(f'Loading {self.table} dimension table has started')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        if self.truncate:
            self.log.info(f'{self.table} is being truncated')
            redshift.run(f'TRUNCATE TABLE {self.table}')
        
        columns = ','.join(self.column_list)
        sql_stmt = f"INSERT INTO {self.table} ({columns}) " + self.sql_query
        redshift.run(sql_stmt)   

        self.log.info(f'{self.table} dimension table has been loaded')