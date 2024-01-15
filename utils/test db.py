import psycopg2
import os

def test_connection(connection_string):
    try:
        # Connect to your postgres DB
        conn = psycopg2.connect(connection_string)

        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Execute a query
        cur.execute("SELECT version();")

        # Retrieve query results
        records = cur.fetchone()
        print(f"Connected to: {records}")

        # Close cursor and connection
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    db_connection_string = "postgresql://postgres:root@localhost:5432/onchain"
    test_connection(db_connection_string)
