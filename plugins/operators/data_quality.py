from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 users_table="",
                 world_bank_stats_table="",
                 test_query_1="",
                 test_query_2="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.users_table = users_table
        self.world_bank_stats_table = world_bank_stats_table
        self.test_query_1 = test_query_1
        self.test_query_2 = test_query_2

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #Customers Table Test
        #running a test that the users table has more than 0 records
        records = redshift.get_records(self.test_query_1)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. Customers table returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. Customers table contained 0 rows")
        self.log.info("Data quality on table Customers check passed")

        #World Bank Stats Table Test
        #running a test that the users table has more than 0 records
        records = redshift.get_records(self.test_query_2)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. WorldBank Stats table returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. WorldBank Stats table contained 0 rows")
        self.log.info("Data quality on table WorldBank Stats check passed")