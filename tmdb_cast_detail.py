import requests
import pandas as pd
from sqlalchemy import create_engine
import config

API_KEY = config.api_key
PSQL_LOGIN = config.postgres['login']
PSQL_PASSWORD = config.postgres['password']

engine = create_engine(f'postgresql://{PSQL_LOGIN}:{PSQL_PASSWORD}@localhost:5432/tmdb_db')
q = """
select distinct "id"
from "cast"
where id not in (select distinct "id" from "cast_detail");
"""

with engine.connect() as con:
    rs = con.execute(q)
    id_list = []
    for row in rs:
        id_list.append(row[0])

for id in id_list:
    person_detail = f'https://api.themoviedb.org/3/person/{id}?api_key={API_KEY}&language=en-US'
    person_response = requests.get(person_detail).json()
    detail = pd.DataFrame([person_response])
    try:
        detail = detail[['birthday', 'deathday', 'id', 'name', 'gender', 'biography', 'popularity', 'place_of_birth',
                        'profile_path', 'imdb_id']]
        detail.to_sql('cast_detail', engine, if_exists='append', index=False)
    except KeyError:
        print("KeyError: ", id)
        pass

