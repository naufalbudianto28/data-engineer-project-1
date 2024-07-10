'''
=================================================
Data Engineer Project

Created by  : Ahmad Naufal Budianto

Program ini dibuat untuk melakukan automatisasi data load dari PostgreSQL dan dilakukan pembersihan, 
kemudian dilakukan penjadwalan posting ke ElasticSearch lalu divisualisasi menggunakan Kibana.
 
Adapun dataset berisi hasil survey platfrom Amazon kepada pelanggannya di bulan Juni 2023.
Hasil dari visualiasi nantinya akan digunakan sebagai bahan validasi, evaluasi, dan perencanaan strategi Tim Product Amazon.
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

# Melakukan koneksi terhadap postgres
def fetch_from_postgresql():
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn=psy.connect(conn_string) # Airflow melakukan koneksi dengan postgres.
    df = pd.read_sql("SELECT * FROM table_m3", conn) # Mengambil dataset yang sudah tersimpan di postgres.
    df.to_csv('/opt/airflow/dags/data_raw.csv', index=False) # Menyimpan data raw ke dalam dag.

    # Menutup koneksi ke postgres.
    conn.close()


# Fungsi untuk melakukan Data Cleaning.
def data_cleaning():
    df = pd.read_csv('/opt/airflow/dags/data_raw.csv')
    ## Menghapus duplikasi data.
    df = df.drop_duplicates()
    ## Terdapat kolom yang memiliki nama yang sama, sehingga saya mengganti nama salah satu kolom.
    df = df.rename(columns={'Personalized_Recommendation_Frequency ': 'Recommendation_Received_Frequency'})
    ## Menghapus spasi setelah akhir kata dibeberapa kolom, dan mengubah semua huruf menjadi lowercase.
    df.columns = df.columns.str.strip().str.lower()
    ## Melakukan drop pada missing value (fyi, dataset yang saya miliki sebenarnya tidak memiliki missing value).
    df = df.dropna()
    ## Melakukan penyimpanan dataset yang sudah dibersihkan.
    df.to_csv('/opt/airflow/dags/P2M3_naufal_data_clean.csv', index=False)



# Fungsi untuk melakukan load data csv yang sudah bersih dan dimasukkan ke dalam Elasticsearch
def post_to_elasticsearch():
    es = Elasticsearch("http://elasticsearch:9200") # Airflow melakukan koneksi dengan Elasticsearch.
    data = pd.read_csv('/opt/airflow/dags/data_clean.csv')
    for i,r in data.iterrows():
        doc = r.to_json()
        res = es.index(index="cleancsv", doc_type="doc", body=doc)
        print(res)


default_args = {
    'owner': 'naufal',
    'start_date': dt.datetime(2024, 6, 21, 13, 30, 0) - dt.timedelta(hours=7), # Melakukan start penjadwalan pada 6.30 WIB.
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('P2M3Clean',
         default_args=default_args,
         description='Proses DAG untuk melakukan pengambilan data dari Postgresql, pembersihan data, dan publikasi ke Elasticsearch',
         schedule_interval='30 6 * * *', # Melakukan penjadwalan otomatis setiap jam 6.30 WIB.
         ) as dag:

    # Define tasks
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
