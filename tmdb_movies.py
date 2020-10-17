import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import config

PSQL_LOGIN = config.postgres['login']
PSQL_PASSWORD = config.postgres['password']

API_KEY = config.api_key
MIN_VOTE_COUNT = 10
## Also excluded Documentaries

# Oscars data
mylist = []
for chunk in pd.read_csv('the_oscar_award.csv',  header=0, quotechar='"', chunksize=20000):
    mylist.append(chunk)
oscar_data = pd.concat(mylist, axis=0)
del mylist
engine = create_engine(f'postgresql://{PSQL_LOGIN}:{PSQL_PASSWORD}@localhost:5432/tmdb_db')
connection = engine.raw_connection()
cursor = connection.cursor()


oscar_data = oscar_data[oscar_data['year_film'].between(2010, 2021)]
oscar_data = oscar_data[oscar_data['category'].isin([
                    'ACTOR IN A LEADING ROLE',
                    'ACTOR IN A SUPPORTING ROLE',
                    'ACTRESS IN A LEADING ROLE',
                    'ACTRESS IN A SUPPORTING ROLE',
                    'ANIMATED FEATURE FILM',
                    'ART DIRECTION',
                    'CINEMATOGRAPHY',
                    'COSTUME DESIGN',
                    'DIRECTING',
                    'FILM EDITING',
                    'FOREIGN LANGUAGE FILM',
                    'MAKEUP',
                    'MUSIC (Original Score)',
                    'MUSIC (Original Song)',
                    'BEST PICTURE',
                    'SOUND EDITING',
                    'SOUND MIXING',
                    'VISUAL EFFECTS',
                    'WRITING (Adapted Screenplay)',
                    'WRITING (Original Screenplay)',
                    'MAKEUP AND HAIRSTYLING',
                    'PRODUCTION DESIGN'])]

oscar_data.to_sql('oscars', engine, if_exists='replace')
cursor.close()




engine = create_engine(f'postgresql://{PSQL_LOGIN}:{PSQL_PASSWORD}@localhost:5432/tmdb_db')
connection = engine.raw_connection()
cursor = connection.cursor()

languages_url = f'https://api.themoviedb.org/3/configuration/languages?api_key={API_KEY}'
languages = requests.get(languages_url).json()
language_df = pd.DataFrame(languages)
language_df.to_sql('language_dict', engine, if_exists='replace', index=False)


years = range(2010, 2021)
for year in years:
    discover_url = f'https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&include_adult=false&include_video=false' \
                   f'&primary_release_year={year}&vote_count.gte={MIN_VOTE_COUNT}&without_genres=99'

    print(year, ' - ', datetime.now())

    new_results = True
    page = 1
    while new_results:
        discover = requests.get(discover_url + f"&page={page}").json()
        new_results = discover.get("results", [])


        for result in new_results:
            movie_id = result['id']

            detail_url = f'https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}&language=en-US'
            credits_url = f'https://api.themoviedb.org/3/movie/{movie_id}/credits?api_key={API_KEY}&language=en-US'
            keywords_url = f'https://api.themoviedb.org/3/movie/{movie_id}/keywords?api_key={API_KEY}'

            # movie
            detail_response = requests.get(detail_url).json()
            detail = pd.DataFrame([detail_response])
            detail['tmdbId'] = detail['id']
            detail['year_check'] = year
            detail = detail[['tmdbId', 'budget', 'imdb_id', 'title', 'original_language',
                             'overview', 'popularity', 'poster_path', 'release_date', 'revenue', 'runtime',
                             'status', 'video', 'vote_average', 'vote_count', 'year_check']]
            detail.to_sql('movie', engine, if_exists='append', index=False)

            credits_response = requests.get(credits_url).json()
            keywords_response = requests.get(keywords_url).json()

            # cast
            if len(credits_response['cast']) > 0:
                credits = pd.DataFrame(credits_response['cast'])
                credits['tmdbId'] = movie_id
                credits['year_check'] = year
                credits.to_sql('cast', engine, if_exists='append', index=False)

            # crew
            if len(credits_response['crew']) > 0:
                crew_members = pd.DataFrame(credits_response['crew'])
                crew_members['tmdbId'] = movie_id
                crew_members['year_check'] = year
                plist = []
                crew_members.to_sql('crew', engine, if_exists='append')

            # genre
            if len(detail_response['genres']) > 0:
                genre = pd.DataFrame(detail_response['genres'])
                genre['tmdbId'] = detail_response['id']
                genre['genre_name'] = genre['name']
                genre = genre[['tmdbId', 'genre_name']]
                genre['year_check'] = year
                genre.to_sql('genre', engine, if_exists='append')

            # country
            if len(detail_response['production_countries']) > 0:
                production_country = pd.DataFrame(detail_response['production_countries'])
                production_country['tmdbId'] = detail_response['id']
                production_country['year_check'] = year
                production_country.to_sql('country', engine, if_exists='append')

            # language
            if len(detail_response['spoken_languages']) > 0:
                languages = pd.DataFrame(detail_response['spoken_languages'])
                languages['tmdbId'] = detail_response['id']
                languages['year_check'] = year
                languages.to_sql('language', engine, if_exists='append')

            #keywords
            if len(keywords_response['keywords']) > 0:
                keywords = pd.DataFrame(keywords_response['keywords'])
                keywords['tmdbId'] = movie_id
                keywords['year_check'] = year
                keywords.to_sql('keywords', engine, if_exists='append')

        page += 1

cursor.close()

print('Complete: ', datetime.now())

