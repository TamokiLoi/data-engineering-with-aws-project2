import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Loads data into staging tables from S3 buckets.

    Args:
    cur: Cursor object for executing PostgreSQL commands.
    conn: Connection object representing the database connection.
    """
    for query in copy_table_queries:
        try:
            print(f"Executing query: {query}")
            cur.execute(query)
            conn.commit()
            print("Query executed successfully.")
        except Exception as e:
            print(f"Error executing query: {query}")
            print(e)

def insert_tables(cur, conn):
    """
    Inserts data from staging tables into analytics tables.

    Args:
    cur: Cursor object for executing PostgreSQL commands.
    conn: Connection object representing the database connection.
    """
    for query in insert_table_queries:
        try:
            print(f"Executing query: {query}")
            cur.execute(query)
            conn.commit()
            print("Query executed successfully.")
        except Exception as e:
            print(f"Error executing query: {query}")
            print(e)

def main():
    """
    Main function of the ETL process.
    Connects to the database, loads data into staging tables, inserts data into analytics tables, and closes the database connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        # Connect to the database using the configuration information in the dwh.cfg file
        print("Connecting to the database...")
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        print("Connection established.")
        
        # Load data into staging tables
        print("Loading data into staging tables...")
        load_staging_tables(cur, conn)
        
        # Insert data into analytics tables
        print("Inserting data into analytics tables...")
        insert_tables(cur, conn)

        # Close the connection to the database
        print("Closing the database connection...")
        conn.close()
        print("Database connection closed.")
    except Exception as e:
        print("Error in main function:")
        print(e)

# Check if the module is being run directly or imported
if __name__ == "__main__":
    main()
