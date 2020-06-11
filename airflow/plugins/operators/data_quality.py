from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        for s_table in self.tables:
            self.log.info(f'Checking table {s_table}...')
            records = redshift.get_records(f"SELECT COUNT(*) FROM {s_table}")
            s_err = f"    ...Data quality check failed. "
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(s_err + f"{s_table} returned no results")
            if records[0][0] < 1:
                raise ValueError(s_err + f"{s_table} contained 0 rows")
            self.log.info(f"...Data quality on table {s_table} check passed "
                          f"with {records[0][0]} records")
