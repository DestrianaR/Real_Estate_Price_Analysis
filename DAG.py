'''
========================================================================================================================================
The purpose of developing this software is to streamline the process of transferring data from PostgreSQL to ElasticSearch automatically.
The dataset utilized pertains to real estate sales across Australia.
========================================================================================================================================

'''

import datetime as dt
import re
import pandas as pd
import psycopg2 as db

from elasticsearch import Elasticsearch

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def get_data_from_postgresql():
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn=db.connect(conn_string)
    df=pd.read_sql('select * from table_m3', conn)
    df.to_csv('/opt/airflow/dags/P2M3_destriana_ramadani_data_clean.csv')

def data_cleaning():
    # Read csv
    df=pd.read_csv('/opt/airflow/dags/P2M3_destriana_ramadani_data_clean.csv')
    # Remove duplicate rows
    df.drop_duplicates(inplace=True)
    # Remove everything except letters in colums name
    df.columns = [re.sub(r'[^a-zA-Z ]+', '', x) for x in df.columns]
    # Lower case columns name 
    df.columns = [x.lower() for x in df.columns]
    # Remove whitespace at both ends
    df.columns = df.columns.str.strip()
    # Replace whatespace in the middle colums name to underscore
    df.columns = df.columns.str.replace(' ', '_') 
    # Change column `date` data type to datetime
    df['date'] = pd.to_datetime(df['date'], dayfirst=True)
    # Drop missing value 
    df.dropna(inplace=True)
    # Change column `postcode`, `bedroom` ,`bathroom`, 'car` ,`yearbuilt`, and `propertycount` data type to int
    df[['postcode', 'bedroom' ,'bathroom', 'car' ,'yearbuilt', 'propertycount']] = df[['postcode', 'bedroom' ,'bathroom', 'car' ,'yearbuilt', 'propertycount']].astype(int)
    # Drop 'unnamed' columns
    df.drop(['unnamed'], axis=1, inplace=True)
    # Create house_id
    df.reset_index(inplace=True)
    df = df.rename(columns = {'index':'house_id'})
    # Save clean data
    df.to_csv('/opt/airflow/dags/P2M3_destriana_ramadani_data_clean.csv', index=False)

def post_to_elasticsearch():
    es = Elasticsearch('http://elasticsearch:9200') 
    df=pd.read_csv('/opt/airflow/dags/P2M3_destriana_ramadani_data_clean.csv')
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="data_milestone", doc_type="doc", body=doc)
        print(res)

default_args = {
    'owner': 'destri',
    'start_date': dt.datetime(2024, 3, 24, 12, 30, 0) -  dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG('DAG_milestone_3',
         default_args=default_args,
         schedule_interval = '30 6 * * *'
         ) as dag:

    fecthFromPostgres = PythonOperator(task_id='GetData',
                                 python_callable=get_data_from_postgresql)
    
    dataCleaning = PythonOperator(task_id='CleaningData',
                                 python_callable=data_cleaning)

    postToElasticsearch = PythonOperator(task_id='PostToElasticsearch',
                                 python_callable=post_to_elasticsearch)

fecthFromPostgres >> dataCleaning >> postToElasticsearch