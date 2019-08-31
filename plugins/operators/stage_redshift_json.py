from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftJSONOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        JSON '{}'; 
    """

    #iam_role 'arn:aws:iam::968940811236:role/dwhRole'
    #DELIMITER '{}'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 ignore_headers=1,
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftJSONOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.ignore_headers = ignore_headers
        self.json_path = json_path

    def execute(self, context):
        self.log.info('StageToRedshiftJSONOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        
        #rendered key was used in the example project
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        rendered_jsonpath_key = self.json_path.format(**context)
        jsonpath = "s3://{}/{}".format(self.s3_bucket, rendered_jsonpath_key)
        formatted_sql = StageToRedshiftJSONOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            jsonpath
        )
        redshift.run(formatted_sql)