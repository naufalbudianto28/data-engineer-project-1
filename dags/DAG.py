'''
=================================================
Data Engineer Project

Created by  : Ahmad Naufal Budianto

This program is created to automate data loading from PostgreSQL, perform cleaning, 
schedule posting to ElasticSearch, and then visualize using Kibana.

The dataset contains the results of a survey conducted by Amazon platform to its customers in June 2023. 
The results from the visualization will later be used as material for validation, evaluation, 
and strategic planning by the Amazon Product Team.
=================================================
'''

import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime as dt
from datetime import timedelta

import psycopg2 as psy
import pandas as pd

from elasticsearch import Elasticsearch

# Connecting to PostgreSQL.
def fetch_from_postgresql():
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn=psy.connect(conn_string) # Airflow connects to PostgreSQL.
    df = pd.read_sql("SELECT * FROM table_m3", conn) # Fetching the dataset stored in PostgreSQL.
    df.to_csv('/opt/airflow/dags/data_raw.csv', index=False) # Storing raw data into the DAG.

    # Closing the connection to PostgreSQL.
    conn.close()


# Function for Data Cleaning.
def data_cleaning():
    df = pd.read_csv('/opt/airflow/dags/data_raw.csv')
    ## Removing duplicate data.
    df = df.drop_duplicates()
    ## There are columns with the same name, so I renamed one of the columns.
    df = df.rename(columns={'Personalized_Recommendation_Frequency ': 'Recommendation_Received_Frequency'})
    ## Removing trailing spaces in several columns and converting all text to lowercase.
    df.columns = df.columns.str.strip().str.lower()
    ## Dropping missing values (FYI, the dataset I have actually does not have missing values).
    df = df.dropna()
    ## Saving the cleaned dataset.
    df.to_csv('/opt/airflow/dags/data_clean.csv', index=False)



# Function to load the cleaned CSV data into Elasticsearch.
def post_to_elasticsearch():
    es = Elasticsearch("http://elasticsearch:9200") # Airflow connects to Elasticsearch.
    data = pd.read_csv('/opt/airflow/dags/data_clean.csv')
    for i,r in data.iterrows():
        doc = r.to_json()
        res = es.index(index="cleancsv", doc_type="doc", body=doc)
        print(res)


default_args = {
    'owner': 'naufal',
    'start_date': dt.datetime(2024, 6, 21, 13, 30, 0) - dt.timedelta(hours=7), # Starting the scheduling at 6:30 AM WIB.
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('P2M3Clean',
         default_args=default_args,
         description='Proses DAG untuk melakukan pengambilan data dari Postgresql, pembersihan data, dan publikasi ke Elasticsearch',
         schedule_interval='30 6 * * *', # Scheduling automation every day at 6:30 AM WIB.
         ) as dag:

    # Define tasks.
    fetch = PythonOperator(
        task_id='fetch_from_postgresql',
        python_callable=fetch_from_postgresql,
    )

    clean = PythonOperator(
        task_id='data_cleaning',
        python_callable=data_cleaning,
    )

    post = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch,
    )

fetch >> clean >> post
