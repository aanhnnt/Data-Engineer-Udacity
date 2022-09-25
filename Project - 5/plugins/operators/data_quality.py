from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Runs data quality check by passing test SQL query and expected result
    
    :param redshift_conn_id: Redshift connection ID
    :param test_queries: SQL query to run on Redshift data warehouse
    :param expected_result: Expected result to match against result of
        test_queries
    """
    
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
    
    def execute(self, context):
        
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Running test")

        test_queries = []
        for table in self.tables:
            if table == "songplays":
                test_queries.append("SELECT COUNT(*) FROM songplays WHERE start_time IS NULL;")
            if table == "users":
                test_queries.append("SELECT COUNT(*) FROM users WHERE user_id IS NULL;")
            if table == "songs":
                test_queries.append("SELECT COUNT(*) FROM songs WHERE song_id IS NULL;")
            if table == "artists":
                test_queries.append("SELECT COUNT(*) FROM artists WHERE artist_id IS NULL;")
            if table == "time":
                test_queries.append("SELECT COUNT(*) FROM time WHERE start_time IS NULL;")

        for query in test_queries:
            records = redshift_hook.get_records(query)
            if records[0][0] != 0:
                raise ValueError(f"""
                    Data quality check failed on: \n {query}
                """)
            else:
                self.log.info("Data quality check passed")
