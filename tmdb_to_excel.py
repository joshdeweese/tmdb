from sqlalchemy import create_engine
import pandas as pd
import config

PSQL_LOGIN = config.postgres['login']
PSQL_PASSWORD = config.postgres['password']

engine = create_engine(f'postgresql://{PSQL_LOGIN}:{PSQL_PASSWORD}@localhost:5432/tmdb_db')
connection = engine.raw_connection()
cursor = connection.cursor()

tables_query = """
SELECT viewname FROM pg_views WHERE schemaname = 'public';
"""

excel_path = f"/Users/deweesej/OneDrive - Memorial Sloan Kettering Cancer Center/TMDB_data/movie_data.xlsx"
writer = pd.ExcelWriter(excel_path)
with engine.connect() as con:
    rs = con.execute(tables_query)
    for table in rs:
        df = pd.read_sql_table(table[0], con=engine)
        df.to_excel(writer, table[0], index=False)
writer.save()









