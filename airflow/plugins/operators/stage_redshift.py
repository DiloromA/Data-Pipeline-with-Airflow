from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TIMEFORMAT as 'epochmillisecs'
        JSON 'auto ignorecase'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id ='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.execution_date = kwargs.get('execution_date')        


    def execute(self, context):
        self.log.info('Execution has started')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        if self.table:
            self.log.info('Clearing data from destination Redshift table')
            redshift.run(f'DELETE FROM {self.table}')

        self.log.info('Copying data from S3 to Redshift')
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        
        self.log.info(f"Pulling staging file for table {self.table} from location : {s3_path}")
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path, 
            credentials.access_key, 
            credentials.secret_key
        )
        
        redshift.run(formatted_sql)
        self.log.info(f"Copying {self.table} to Redshift is complete")
        




