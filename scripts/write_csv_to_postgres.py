"""
Downloads the csv file from the URL. Creates a new table in the Postgres server.
Reads the file as a dataframe and inserts each record to the Postgres table. 
"""
import psycopg2
import os
import traceback
import logging
import pandas as pd
from dotenv import load_dotenv
import urllib.request

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

# Load environment variables from .env file
load_dotenv()

postgres_host = os.environ.get('postgres_host')
postgres_database = os.environ.get('postgres_database')
postgres_user = os.environ.get('postgres_user')
postgres_password = os.environ.get('postgres_password')
postgres_port = os.environ.get('postgres_port')

destination_path = "/Users/charlesdecian/Documents/postgre/postgre-airflow/churn_modelling.csv"

try:
    conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password,
        port=postgres_port
    )
    cur = conn.cursor()
    logging.info('Postgres server connection is successful')
except Exception as e:
    traceback.print_exc()
    logging.error("Couldn't create the Postgres connection")

def verify_csv_file(file_path: str):
    """
    Verify that the CSV file exists in the specified location
    """
    try:
        if os.path.exists(file_path):
            logging.info('CSV file found successfully')
            return True
        else:
            logging.error(f'CSV file not found at: {file_path}')
            return False
    except Exception as e:
        logging.error(f'Error while checking CSV file: {e}')
        traceback.print_exc()
        return False

def create_postgres_table():
    """
    Create the Postgres table with a desired schema
    """
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling (RowNumber INTEGER PRIMARY KEY, CustomerId INTEGER, 
        Surname VARCHAR(50), CreditScore INTEGER, Geography VARCHAR(50), Gender VARCHAR(20), Age INTEGER, 
        Tenure INTEGER, Balance FLOAT, NumOfProducts INTEGER, HasCrCard INTEGER, IsActiveMember INTEGER, EstimatedSalary FLOAT, Exited INTEGER)""")
        conn.commit()  # Add commit here
        logging.info(' New table churn_modelling created successfully to postgres server')
        return True  

    except Exception as e:
        conn.rollback()  # Add rollback on error
        logging.error(f'Error creating table: {str(e)}')
        return False


def write_to_postgres():
    """
    Create the dataframe and write to Postgres table if it doesn't already exist
    """
    df = pd.read_csv(destination_path)  
    inserted_row_count = 0

    for _, row in df.iterrows():
        count_query = f"""SELECT COUNT(*) FROM churn_modelling WHERE RowNumber = {row['RowNumber']}"""
        cur.execute(count_query)
        result = cur.fetchone()
        
        if result[0] == 0:
            inserted_row_count += 1
            cur.execute("""INSERT INTO churn_modelling (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, 
            Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)""", 
            (
                row['RowNumber'], 
                row['CustomerId'], 
                row['Surname'],
                row['CreditScore'],
                row['Geography'],
                row['Gender'],
                row['Age'],
                row['Tenure'],
                row['Balance'],
                row['NumOfProducts'],
                row['HasCrCard'],
                row['IsActiveMember'],
                row['EstimatedSalary'],
                row['Exited']
            ))
    
    conn.commit()  
    logging.info(f' {inserted_row_count} rows from csv file inserted into churn_modelling table successfully')

def verify_table_data():
    """
    Verify table exists and contains data
    """
    try:
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'churn_modelling'
            );
        """)
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            logging.error("Table does not exist!")
            return False
            
        # Check row count
        cur.execute("SELECT COUNT(*) FROM churn_modelling;")
        row_count = cur.fetchone()[0]
        logging.info(f"Table contains {row_count} rows")
        
        # Sample some data
        cur.execute("SELECT * FROM churn_modelling LIMIT 5;")
        sample_data = cur.fetchall()
        logging.info(f"Sample data: {sample_data}")
        
        return True
        
    except Exception as e:
        logging.error(f"Error verifying table data: {str(e)}")
        return False
    
def write_csv_to_postgres_main():
    if not verify_csv_file(destination_path):
        logging.error("CSV file verification failed. Stopping execution.")
        return
    
    table_created = create_postgres_table()
    if not table_created:
        logging.error("Failed to create table. Stopping execution.")
        return
    
    try:
        write_to_postgres()
        verify_table_data()  
    except Exception as e:
        conn.rollback()
        logging.error(f"Error during data insertion: {str(e)}")
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    write_csv_to_postgres_main()
