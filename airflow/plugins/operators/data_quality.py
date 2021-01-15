from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 data_quality_checks = [],
                 tables = [],
                 expected_result = '',
                 sql_query = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks
        self.tables = tables
        self.expected_result = expected_result
        self.sql_query = sql_query

        
    def execute(self, context):
        self.log.info('DataQualityOperator implemention has started')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        for table in self.tables:
            records = redshift.get_records(self.sql_query.format(table=table))
        
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data quality check for {table} table failed.')
            record_count = records[0][0]
            if self.expected_result=="greater than 0":
                if record_count < 1:
                    raise ValueError(f'Data quality check for {table} table failed. It contains 0 rows.')
                self.log.info(f'Data quality check for {table} table passed with {records[0][0]} records')
            