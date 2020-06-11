from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 table='',
                 sql_query='',
                 delete_load=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.sql_query = sql_query
        self.delete_load = delete_load

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        if self.delete_load:
            self.log.info(f'Delete existing data from table {self.table}...')
            redshift.run(F'DELETE FROM {self.table}')

        self.log.info(f'Insert data into table {self.table}...')
        s_sql2use = LoadDimensionOperator.insert_sql.format(
                self.table,
                self.sql_query
            )
        redshift.run(s_sql2use)
