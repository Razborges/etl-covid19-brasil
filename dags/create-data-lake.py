from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import pandas as pd

default_args = {
  'ownew': 'Razborges',
  'depends_on_past': False,
  'start_date': datetime(2020, 12, 30, 18, 10),
  'email': ['airflow@airflow.com'],
  'email_on_failure': False,
  'email_on_retry': False
}

@dag(
  default_args=default_args,
  schedule_interval=None,
  description='Criando Data Lake com dados do Covid-19 Brasil e Microregiões do Brasil'
)
def create_data_lake():
  '''
    Obtém dados de uma planilha Nacional sobre o Covid-19 do Brasil e dados sobre as Microregiões do Brasil no IBGE
    para popular um Data Lake Postgres para consultas.
  '''

  def conn_postgres():
    import psycopg2

    POSTGRES_USER = Variable.get('POSTGRES_USER')
    POSTGRES_PASS = Variable.get('POSTGRES_PASS')
    POSTGRES_DBASE = Variable.get('POSTGRES_DBASE')
    POSTGRES_HOST = Variable.get('POSTGRES_HOST')

    conn = psycopg2.connect(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}/{POSTGRES_DBASE}')
    return conn

  def df_to_database(conn, df, table_name, if_exists='replace', encoding='utf-8'):
    from io import StringIO
    from sqlalchemy import create_engine

    POSTGRES_USER = Variable.get('POSTGRES_USER')
    POSTGRES_PASS = Variable.get('POSTGRES_PASS')
    POSTGRES_DBASE = Variable.get('POSTGRES_DBASE')
    POSTGRES_HOST = Variable.get('POSTGRES_HOST')

    connAux = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}/{POSTGRES_DBASE}')
    # Create Table
    logging.info('******** Create table...')
    df[:0].to_sql(table_name, connAux, if_exists=if_exists, index=False)

    # Prepare data
    logging.info('******** Create buffer...')
    buffer = StringIO()
    df.to_csv(buffer, sep=',', header=False, encoding=encoding, index=False)
    buffer.seek(0)

    logging.info('******** Insert data...')
    cursor = conn.cursor()
    cursor.copy_from(buffer, table_name, sep=",")
    conn.commit()
    logging.info('******** End copy data')

  @task
  def get_data_covid():
    import numpy as np
    PATH_FILE = '/usr/local/airflow/data/caso_full.csv'
    
    logging.info('******** Read file csv covid...')
    df = pd.read_csv(PATH_FILE)
    df['created_at_datalake'] = datetime.today()

    logging.info('******** Prepare data...')
    
    df_parser = df.dropna(subset=['city', 'city_ibge_code'])
    df_parser['last_available_confirmed_per_100k_inhabitants'] = df_parser['last_available_confirmed_per_100k_inhabitants'].replace({
      '': 0,
      ' ': 0,
      None: 0,
      np.NaN: 0
    })
    
    df_parser['city_ibge_code'] = df_parser['city_ibge_code'].astype(np.int64)

    logging.info('******** Start save db...')
    conn = conn_postgres()
    df_to_database(conn, df_parser, 'covid')
    conn.close()
  
  @task
  def get_data_api():
    import requests
    import json

    PATH_IBGE_CITIES = Variable.get('PATH_IBGE_CITIES')

    logging.info('******** Request IBGE data...')
    response = requests.get(PATH_IBGE_CITIES)
    resjson = json.loads(response.text)
    logging.info('******** Normalize json...')
    parser = pd.json_normalize(resjson)

    df = pd.DataFrame(parser)
    df['created_at_datalake'] = datetime.today()

    logging.info('******** Start save db...')
    conn = conn_postgres()
    df_to_database(conn, df, 'microrregioes')
    conn.close()


  covid = get_data_covid()
  api = get_data_api()

data_lake = create_data_lake()
