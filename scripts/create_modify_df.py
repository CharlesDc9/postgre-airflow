"""
Downloads the csv file from the URL. Creates a new table in the Postgres server.
Reads the file as a dataframe and inserts each record to the Postgres table. 
"""
import psycopg2
import os
import traceback
import logging
import numpy as np
import pandas as pd
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

load_dotenv()

postgres_host = os.environ.get('postgres_host')
postgres_database = os.environ.get('postgres_database')
postgres_user = os.environ.get('postgres_user')
postgres_password = os.environ.get('postgres_password')
postgres_port = os.environ.get('postgres_port')

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

def create_base_df(cur):
    """
    Base dataframe of churn_modelling table
    """
    cur.execute("""SELECT * FROM churn_modelling""")
    rows = cur.fetchall()

    col_names = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=col_names)

    # Drop rownumber column
    df = df.drop('rownumber', axis=1)
    
    # Create null values
    index_to_be_null = np.random.randint(10000, size=30)
    df.loc[index_to_be_null, ['balance','creditscore','geography']] = np.nan
    
    # Fill null values - updated to avoid warnings
    most_occured_country = df['geography'].value_counts().index[0]
    df = df.assign(geography=df['geography'].fillna(most_occured_country))
    
    avg_balance = df['balance'].mean()
    df = df.assign(balance=df['balance'].fillna(avg_balance))

    median_creditscore = df['creditscore'].median()
    df = df.assign(creditscore=df['creditscore'].fillna(median_creditscore))

    return df


def create_creditscore_df(df):
    df_creditscore = df[['geography', 'gender', 'exited', 'creditscore']].groupby(['geography','gender']).agg({'creditscore':'mean', 'exited':'sum'})
    df_creditscore.rename(columns={'exited':'total_exited', 'creditscore':'avg_credit_score'}, inplace=True)
    df_creditscore.reset_index(inplace=True)

    df_creditscore.sort_values('avg_credit_score', inplace=True)

    return df_creditscore


def create_exited_salary_correlation(df):
    df_salary = df[['geography','gender','exited','estimatedsalary']].groupby(['geography','gender']).agg({'estimatedsalary':'mean'}).sort_values('estimatedsalary')
    df_salary.reset_index(inplace=True)

    min_salary = round(df_salary['estimatedsalary'].min(),0)
    
    # Convert all values to Python native types
    df_exited_salary_correlation = pd.DataFrame({
        'exited': df['exited'].astype(int),
        'is_greater': (df['estimatedsalary'] > df['estimatedsalary'].min()).astype(int),
        'correlation': np.where(df['exited'] == (df['estimatedsalary'] > df['estimatedsalary'].min()), 1, 0)
    })

    # Convert all columns to Python int
    return df_exited_salary_correlation.apply(lambda x: x.astype(int))

def create_exited_age_correlation(df):
    df_exited_age_correlation = df.groupby(['geography', 'gender', 'exited']).agg({
    'age': 'mean',
    'estimatedsalary': 'mean',
    'exited': 'count'
    }).rename(columns={
        'age': 'avg_age',
        'estimatedsalary': 'avg_salary',
        'exited': 'number_of_exited_or_not'
    }).reset_index().sort_values('number_of_exited_or_not')

    return df_exited_age_correlation


def create_new_tables_in_postgres():
    try:
        # Drop existing tables
        cur.execute("DROP TABLE IF EXISTS churn_modelling_creditscore CASCADE")
        cur.execute("DROP TABLE IF EXISTS churn_modelling_exited_age_correlation CASCADE")
        cur.execute("DROP TABLE IF EXISTS churn_modelling_exited_salary_correlation CASCADE")
        conn.commit()
        
        # Create tables
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_creditscore 
            (geography VARCHAR(50), gender VARCHAR(20), avg_credit_score FLOAT, total_exited INTEGER)""")
        
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_age_correlation 
            (geography VARCHAR(50), gender VARCHAR(20), exited INTEGER, avg_age FLOAT, 
            avg_salary FLOAT, number_of_exited_or_not INTEGER)""")
        
        cur.execute("""CREATE TABLE IF NOT EXISTS churn_modelling_exited_salary_correlation 
            (exited INTEGER, is_greater INTEGER, correlation INTEGER)""")
        
        conn.commit()
        logging.info("Tables created successfully")
        
        # Create base dataframe
        df = create_base_df(cur)
        
        # Insert creditscore data
        df_creditscore = create_creditscore_df(df)
        for _, row in df_creditscore.iterrows():
            cur.execute("""
                INSERT INTO churn_modelling_creditscore 
                (geography, gender, avg_credit_score, total_exited) 
                VALUES (%s, %s, %s, %s)
            """, (
                str(row['geography']), 
                str(row['gender']), 
                float(row['avg_credit_score']), 
                int(row['total_exited'])
            ))
        
        # Insert age correlation data
        df_age = create_exited_age_correlation(df)
        for _, row in df_age.iterrows():
            cur.execute("""
                INSERT INTO churn_modelling_exited_age_correlation 
                (geography, gender, exited, avg_age, avg_salary, number_of_exited_or_not) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                str(row['geography']), 
                str(row['gender']), 
                int(row['exited']), 
                float(row['avg_age']), 
                float(row['avg_salary']), 
                int(row['number_of_exited_or_not'])
            ))
        
        # Insert salary correlation data
        df_salary = create_exited_salary_correlation(df)
        for index, row in df_salary.iterrows():
            cur.execute("""
                INSERT INTO churn_modelling_exited_salary_correlation 
                (exited, is_greater, correlation) 
                VALUES (%s, %s, %s)
            """, (
                int(row['exited'].item()),  # Convert numpy.int32 to Python int
                int(row['is_greater'].item()),
                int(row['correlation'].item())
            ))
        
        conn.commit()
        logging.info("Data inserted into all tables successfully")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error in create_new_tables_in_postgres: {str(e)}")
        traceback.print_exc()

def main():
    try:
        df = create_base_df(cur)
        create_new_tables_in_postgres()
        
        # Verify tables were created and populated
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = cur.fetchall()
        logging.info(f"Available tables: {[table[0] for table in tables]}")
        
        # Log row counts
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cur.fetchone()[0]
            logging.info(f"Table {table[0]} has {count} rows")
            
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    main()