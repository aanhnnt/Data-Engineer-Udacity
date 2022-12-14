from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Loads dimension table in Redshift from data in staging table(s)
    
    :param redshift_conn_id: Redshift connection ID
    :param table: Target table in Redshift to load
    :param select_sql: SQL query for getting data to load into target table
    :param append_insert: Whether the append-insert or truncate-insert method
        of loading should be used
    :param primary key: When using the append-insert method, the column to check
        if the row already exists in the target table. If there is a match, the
        row in the target table will then be updated
    """
    
    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 mode="refresh",
                 *args, **kwargs):
        
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.mode = mode
    
    def execute(self, context):
        
        self.log.info("Getting credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.mode == "refresh":
            table_insert_sql = f"""
                INSERT INTO {self.table}
                {self.select_sql}
            """
            
            self.log.info("Clearing data from dimension table in Redshift")
            redshift_hook.run(f"TRUNCATE TABLE {self.table};")
        
        self.log.info("Loading data into dimension table in Redshift")
        redshift_hook.run(table_insert_sql)
