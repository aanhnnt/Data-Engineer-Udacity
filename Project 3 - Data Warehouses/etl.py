import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data to staging tables by executing all queries in `copy_table_queries` list.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Insert data to dim and fact tables by executing all queries in `insert_table_queries` list.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Gets config information from dwh.cfg
    - Connects to Redshift Cluster
    - Load insert data to dim and fact tables
    - Closes the connection
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
                            config['DB']['HOST'],
                            config['DB']['DB_NAME'],
                            config['DB']['DB_USER'],
                            config['DB']['DB_PASSWORD'],
                            config['DB']['DB_PORT']))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()