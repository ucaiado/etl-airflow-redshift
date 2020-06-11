from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM {}
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON {}
        MAXERROR AS 250
        COMPUPDATE OFF REGION 'us-west-2';
    """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",
                 json_data="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.postgres_conn_id = postgres_conn_id
        self.s3_path = s3_path
        self.json_data = json_data
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        self.log.info(f'Delete existing data from table {self.table}...')
        redshift.run(F'DELETE FROM {self.table}')

        self.log.info(f'Insert data from S3 to table {self.table}...')
        s_sql2use = StageToRedshiftOperator.copy_sql.format(
                self.table,
                self.s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_data
            )
        redshift.run(s_sql2use)
