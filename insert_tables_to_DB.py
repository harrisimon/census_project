
import io
import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()
migration_csv_path = './census_migration_data.csv'
state_pop_path = './state_populations.csv'
census_class_path = 'census_classification.csv'

migration = pd.read_csv(migration_csv_path, sep=',')
state_pop = pd.read_csv(state_pop_path, sep=',')
census_class = pd.read_csv(census_class_path, sep=',')



def write_to_postgres(df, df_name):
    url = os.environ["DATABASE_URL"]
        # "postgresql+psycopg2://rgxurjfq:tmVGy6x7jc8OOmlCm2UhASwOSaP8z3Hn@isilo.db.elephantsql.com/rgxurjfq"
    engine = create_engine(url)
    print(df_name)
    # df.head(0).to_sql(f"{df_name}", engine, if_exists='replace', index=False)
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, f"{df_name}", null='')
    print('copied')
    conn.commit()
    cur.close()
    conn.close()

def run_insert():
    write_to_postgres(census_class,'census_classifications')
    write_to_postgres(migration,'census_migration_data')
    write_to_postgres(state_pop,'state_populations')







