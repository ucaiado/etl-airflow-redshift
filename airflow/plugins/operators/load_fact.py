from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 table='',
                 sql_query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        self.log.info(f'Delete existing data from table {self.table}...')
        redshift.run(F'DELETE FROM {self.table}')

        self.log.info(f'Insert data into table {self.table}...')
        s_sql2use = LoadFactOperator.insert_sql.format(
                self.table,
                self.sql_query
            )
        redshift.run(s_sql2use)
