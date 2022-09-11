import time
import pandas as pd
import psycopg2
from datetime import datetime
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine


def data_clean():
     # Endereço stage area, dados de entrada(df_census_in), dados de saida(df_census_ou)
     stage_area = Variable.get("stage_area")
     file_in = stage_area+"df_census_in.csv"
     file_out = stage_area+"df_census_clean.csv"

     try:
        df = pd.read_csv(file_in,
                     names=['age', 'workclass', 'fnlwgt', 'education', 'education-num', 'marital-status', 'occupation', 
                            'relationship', 'race', 'sex', 'capital-gain', 'capital-loss', 'hours-per-week', 'native-country',
                            'class']
                     )
        # data analysis
        df.head()
        df.info()
        df.describe()

        # verificando dados nulos
        df.isnull().sum()

        # Identificando dados "errado" - dtype
        # coluna 'age'
        age_error = df['age'].loc[~df['age'].str.match(r'[0-9.]+')]
        # coluna fnlwgt
        fnlwgt_error = df['fnlwgt'].loc[df['fnlwgt'].str.contains('[a-zA-z]')]
        # .... outras colunas

        # removendo linha(s) que contém dados inválidos da coluna 'age'
        df = df.loc[df["age"].loc[~df['age'].str.contains('[a-zA-Z]+')].index]
        # removendo linha(s) que contém dados inválidos da coluna 'fnlwgt'
        df = df.loc[df["fnlwgt"].loc[~df['fnlwgt'].str.contains('[a-zA-Z]+')].index]
        # removendo linha(s) que contém dados inválidos da coluna 'fnlwgt'
        df = df.loc[df["capital-gain"].loc[~df['capital-gain'].str.contains('[a-zA-Z]+')].index]
     
        # novas dimensões do dataframa
        print("dataframe clean: ", df.shape)
     
        #  Salva o dataframe limpo na stage area
        try:
            df.to_csv(file_out, index=False)

        except ConnectionError:
            msg = "Error save file in stage area .............."
            print(msg) # Erro na transferência do arquivo csv na stage

     except ConnectionError:
        msg = "Error open csv file"
        print(msg) # Erro open csv file...

 
def init_db():
    # conexão ao database, schema: csv_bd 
    pg_hook = PostgresHook(
              postgres_conn_id='postgres_default',
              schema='csv_bd'
              )
    # sql da criação da tabela us-census
    sql = """CREATE TABLE IF NOT EXISTS US_CENSUS(
             age INT,
             workclass VARCHAR(25),
             fnlwgt INT,  
             education  VARCHAR(20),
             education_num  INT,
             marital_status VARCHAR(25),
             occupation     VARCHAR(25),
             relationship   VARCHAR(20),
             race    VARCHAR(25),
             sex     VARCHAR(10),
             capital_gain    INT,
             capital_loss    INT,
             hours_per_week  INT,
             native_country  VARCHAR(20), 
             class   VARCHAR(5));"""

    # conexão postgres
    try:
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()

        # criação da tabela
        try:
            cursor.execute(sql)
            pg_conn.commit()

        except ConnectionError:
            msg = "Error database error .."
            print(msg) # Erro no database ...

    except ConnectionError:
        msg = "Error database connection error"
        print(msg) # Erro no psycpg2  ...

def load_census_data():
    # carga dos dados:  biblioteca "pandas dataframe to postgres", que utiliza SqlAlchemy (from sqlalchemy import create_engine)
    conn_string = 'postgresql://postgres:postgres@localhost/csv_bd'

    # connection database setup
    try: 
        db = create_engine(conn_string)
        conn = db.connect()
        conn.autocommit = True
        # arquivo csv_clean da stage area
        stage_area = Variable.get("stage_area")
        df_clean = stage_area+"df_census_clean.csv"
        df = pd.read_csv(df_clean, header=[0])

        df_rows = len(df)

        # controle e transferência de dados: chunks e time
        # chunks de dados
        row_id = 0
        chunk = 1630
        # bloco de 1630 linhas
        data = df.iloc[row_id:row_id+chunk]
    
        # transferência de dados
        print("carga dos dados...")
        try: 
            while (row_id < df_rows):
                data.to_sql('us_census', con=conn, if_exists='append', index=False)

                # aguarda 10 segundos 
                time.sleep(10)

                # pega próximo bloco de dados
                row_id = row_id+chunk
                data = df.iloc[row_id:row_id+chunk]
                print(".......... transferência de ",row_id," registros")

        except ConnectionError:
            msg = "Error database connection error"
            print(msg) # Erro no psycpg2 + sqlalchemy  ...

    except ConnectionError:
        msg = "Error sql engine error"
        print(msg) # Erro no psycpg2 + sqlalchemy  ...


with DAG(
    dag_id='census_postgres_dag',
    schedule_interval='@daily',
    start_date=datetime(year=2022, month=2, day=1),
    catchup=False
    ) as dag:

    # 1. Data cleaning
    task_data_clean = PythonOperator(
          task_id='data_clean',
          python_callable=data_clean
    )

    # 2. Process init db 
    task_init_db = PythonOperator(
          task_id='init_db',
          python_callable=init_db
    )

    # 3. Save to Postgres
    task_load_census_data = PythonOperator(
          task_id="load_census_data",
          python_callable=load_census_data
          )

    task_data_clean >> task_init_db >>  task_load_census_data
