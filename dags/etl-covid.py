from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import pandas as pd

FILE_PATH_COVID = '/tmp/covid.csv'
FILE_PATH_MICRORREGIOES = '/tmp/microrregioes.csv'
FILE_PATH_JOIN_DATA = '/tmp/join-data.csv'

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
  description='Criando Data Mart com dados do Covid-19 Brasil e Microregiões do Brasil, consolidando as informações importantes e salvando no ElasticSearch'
)
def etl_covid():
  '''
    Obtém dados de uma base PostGres, uma tabela com as informações completas sobre o Covid-19 e outra tabela de Microrregiões,
    consolida e filtra as informações importantes e salva em uma base ElasticSearch para ser o Data Mart da aplicação.
  '''

  def conn_postgres():
    from sqlalchemy import create_engine

    POSTGRES_USER = Variable.get('POSTGRES_USER')
    POSTGRES_PASS = Variable.get('POSTGRES_PASS')
    POSTGRES_DBASE = Variable.get('POSTGRES_DBASE')
    POSTGRES_HOST = Variable.get('POSTGRES_HOST')

    conn = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}/{POSTGRES_DBASE}')
    return conn
  
  @task
  def get_data_covid():
    import sqlalchemy as db

    conn = conn_postgres()
    metadata = db.MetaData()
    covid_table = db.Table('covid', metadata, autoload=True, autoload_with=conn)

    query = db.select(
        [covid_table]
      ).where(
          db.and_(
            covid_table.columns.place_type == 'city',
            covid_table.columns.is_last == True
          )
        )
    proxy = conn.execute(query)
    response = proxy.fetchall()

    df = pd.DataFrame(response)
    df.columns = response[0].keys()
    df.to_csv(FILE_PATH_COVID, index=False, encoding='utf-8', sep=';')
    return FILE_PATH_COVID
  
  @task
  def get_data_microrregioes():
    import sqlalchemy as db

    conn = conn_postgres()
    metadata = db.MetaData()
    microrregioes_table = db.Table('microrregioes', metadata, autoload=True, autoload_with=conn)

    query = db.select([microrregioes_table])
    proxy = conn.execute(query)
    response = proxy.fetchall()

    df = pd.DataFrame(response)
    df.columns = response[0].keys()

    df.to_csv(FILE_PATH_MICRORREGIOES, index=False, encoding='utf-8', sep=';')
    return FILE_PATH_MICRORREGIOES
  
  @task
  def join_data(file_covid, file_microrregioes):
    df_covid = pd.read_csv(file_covid, sep=';')
    df_microrregioes = pd.read_csv(
      file_microrregioes,
      sep=';',
      usecols=['id', 'microrregiao.nome', 'microrregiao.mesorregiao.nome', 'microrregiao.mesorregiao.UF.regiao.nome']
    )
    df_microrregioes.columns = ['code', 'microrregiao', 'mesorregiao', 'regiao']

    df_aggregate = pd.merge(df_covid, df_microrregioes, left_on='city_ibge_code', right_on='code', how='left').drop('code', axis=1)

    df_aggregate.to_csv(FILE_PATH_JOIN_DATA, index=False, encoding='utf-8', sep=';')
    return FILE_PATH_JOIN_DATA
  
  @task
  def save_elastic(file_path):
    df = pd.read_csv(file_path, sep=';')
    print(df.head())
  
  covid = get_data_covid()
  microrregioes = get_data_microrregioes()
  data = join_data(covid, microrregioes)
  save = save_elastic(data)

etl_covid = etl_covid()