import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Drops tables in the database.

    Args:
    cur: Cursor object for executing PostgreSQL commands.
    conn: Connection object representing the database connection.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Creates tables in the database.

    Args:
    cur: Cursor object for executing PostgreSQL commands.
    conn: Connection object representing the database connection.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        table_name = query
        print(f"Created table: {table_name}.")

def main():
    """
    Main function of the program.
    Connects to the database, drops existing tables, creates new tables, and closes the database connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the database using the configuration information in the dwh.cfg file
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drop any existing tables
    drop_tables(cur, conn)
    # Create new tables in the database
    create_tables(cur, conn)

    # Close the connection to the database
    conn.close()

# Check if the module is being run directly or imported
if __name__ == "__main__":
    main()
