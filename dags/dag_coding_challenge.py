from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import psycopg2
import gzip
import os
import requests
import pandas as pd
from urllib.parse import urlparse
import numpy as np
import tldextract
import json
import time

from analysis.analysis_functions import *
from analysis.klazify_categories import klazify_categories

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

dag = DAG(
    'common_crawl_analysis',
    default_args=default_args,
    description='common_crawl_analysis',
    schedule_interval=None,
)


# ------------------------------------------------------------------------------------------------

def download_segment_files():
    urls = [
        'https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-30/segments/1720763514387.30/wat/CC-MAIN-20240712094214-20240712124214-00000.warc.wat.gz',
        'https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-30/segments/1720763514387.30/wat/CC-MAIN-20240712094214-20240712124214-00001.warc.wat.gz',
        'https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-30/segments/1720763514387.30/wat/CC-MAIN-20240712094214-20240712124214-00002.warc.wat.gz'
    ]

    os.makedirs('/tmp/common_crawl', exist_ok=True)

    for url in urls:
        print("URLL: ", url)
        response = requests.get(url, stream=True)
        filename = os.path.join('/tmp/common_crawl', url.split('/')[-1])
        with open(filename, 'wb') as f:
            print("FILENAME: ", filename)
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        time.sleep(3)
    print("END")

download_segments = PythonOperator(
    task_id='download_segments',
    python_callable=download_segment_files,
    dag=dag,
)

# download_segments = EmptyOperator(
#     task_id='download_segments',
#     dag=dag,
# )

# ------------------------------------------------------------------------------------------------

def check_and_create_database():
    conn = psycopg2.connect(
        host='postgres',
        user='airflow',
        password='airflow',
        dbname='airflow',
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'common_crawl_db'")
    exists = cur.fetchone()
    
    if not exists:
        cur.execute("CREATE DATABASE common_crawl_db")
    
    cur.close()
    conn.close()

create_db = PythonOperator(
    task_id='create_db',
    python_callable=check_and_create_database,
    dag=dag,
)

# create_db = EmptyOperator(
#     task_id='create_db',
#     dag=dag,
# )


# ------------------------------------------------------------------------------------------------

def extract_wet_to_dataframe(wet_file_path):

    urls = []
    texts = []

    with gzip.open(wet_file_path, 'rt', encoding='utf-8') as file:
        url = None
        text = None

        for line in file:
            if line.startswith('WARC-Target-URI:'):
                url = line.split(' ')[1].strip()
            # Controlla se la lineaa e' vuota indicando la fine dell'entry
            elif line.strip() == '':
                if url and text:
                    #.lower() da capire se mettere nel urls
                    urls.append(url.strip())
                    texts.append(text.strip())
                url = None
                text = None
            else:
                if text is None:
                    text = line.strip()
                else:
                    text += ' ' + line.strip()

    data = {'url': urls}
    df = pd.DataFrame(data)

    return df

def create_and_write_dataframe():

    wat_file_paths = [
        '/tmp/common_crawl/CC-MAIN-20240712094214-20240712124214-00000.warc.wat.gz',
        '/tmp/common_crawl/CC-MAIN-20240712094214-20240712124214-00001.warc.wat.gz',
        '/tmp/common_crawl/CC-MAIN-20240712094214-20240712124214-00002.warc.wat.gz'
        ]


    dataframes = []

    for path in wat_file_paths:
        df = extract_wet_to_dataframe(path)
        dataframes.append(df)
    
    # ci vorrebbe la distinct ma troppo lenta su pc con pandas
    df = pd.concat(dataframes, ignore_index=True)

    engine = create_engine('postgresql://airflow:airflow@postgres:5432/common_crawl_db')
    df.to_sql('url_table', engine, if_exists='replace', index=False)


create_url_table = PythonOperator(
    task_id='create_url_table',
    python_callable=create_and_write_dataframe,
    dag=dag
)

# create_url_table = EmptyOperator(
#     task_id='create_url_table',
#     dag=dag,
# )

# ------------------------------------------------------------------------------------------------


def transform():
    conn = psycopg2.connect(
        host='postgres',
        user='airflow',
        password='airflow',
        dbname='common_crawl_db',
    )
    query = """
    SELECT *
    FROM url_table
    """
    df = pd.read_sql_query(query, conn)

    df['is_homepage'] = df['url'].apply(is_homepage)
    df['homepage'] = df['url'].apply(get_homepage)
    df['subsection'] = df['url'].apply(get_subsection)

    aggregated_df = df.groupby(['homepage', 'is_homepage']).agg(
    count=('homepage', 'size'),
    subsections=('subsection', lambda x: set(x))).reset_index()

    aggregated_df['country'] = aggregated_df['homepage'].apply(get_country)

    aggregated_df['category'] = np.random.choice(klazify_categories, len(aggregated_df))

    aggregated_df['is_ad_based'] = aggregated_df.apply(create_check_ad, axis=1)
    aggregated_df['is_ad_based'].fillna(False, inplace=True)

    print(aggregated_df.head(3))
    print(aggregated_df.info())

    aggregated_df['subsections'] = aggregated_df['subsections'].apply(lambda x: json.dumps(list(x)))

    engine = create_engine('postgresql://airflow:airflow@postgres:5432/common_crawl_db')
    aggregated_df.to_sql('result_table', engine, if_exists='replace', index=False)
    
    conn.close()


execute_transform = PythonOperator(
    task_id='execute_transform',
    python_callable=transform,
    dag=dag,
)


download_segments >> create_db >> create_url_table >> execute_transform