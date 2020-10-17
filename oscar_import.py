import pandas as pd
from sqlalchemy import create_engine
import config

PSQL_LOGIN = config.postgres['login']
PSQL_PASSWORD = config.postgres['password']

mylist = []
for chunk in pd.read_csv('the_oscar_award.csv',  header=0, quotechar='"', chunksize=20000):
    mylist.append(chunk)

oscar_data = pd.concat(mylist, axis=0)
del mylist

engine = create_engine(f'postgresql://{PSQL_LOGIN}:{PSQL_PASSWORD}@localhost:5432/tmdb_db')
connection = engine.raw_connection()
cursor = connection.cursor()

oscar_data.to_sql('oscars', engine, if_exists='replace')

cursor.close()