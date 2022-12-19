import configparser
import psycopg2
from sql_setup_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all tables by executing all queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all tables by executing all queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Gets config information from dwh.cfg
    - Connects to Redshift Cluster
    - Drops (if exists) and Creates tables
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()