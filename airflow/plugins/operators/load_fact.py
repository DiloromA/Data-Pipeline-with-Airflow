from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 sql_query = '',
                 column_list=[],
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.column_list= column_list
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info('LoadFactOperator implementation has started')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        columns = ','.join(self.column_list)
        sql_stmt = f"INSERT INTO {self.table} ({columns}) " + self.sql_query

        redshift.run(sql_stmt)
        self.log.info(f'Fact table {self.table} has been loaded')
