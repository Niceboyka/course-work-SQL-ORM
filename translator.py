import sqlite3
import pandas as pd

# Підключення до БД
conn = sqlite3.connect('database_olympic.db')

# Відкриття csv файла та збереження до БД

# Olympic Athlete Bio
df = pd.read_csv('Olympic_Athlete_Bio.csv')

df.columns = df.columns.str.strip()

df.to_sql('athlete_bio', conn, if_exists='replace')


# Olympic Athlete Event Results
df = pd.read_csv('Olympic_Athlete_Event_Results.csv')

df.columns = df.columns.str.strip()

df.to_sql('athlete_event_results', conn, if_exists='replace')


# Olympic Games Medal Tally
df = pd.read_csv('Olympic_Games_Medal_Tally.csv')

df.columns = df.columns.str.strip()

df.to_sql('games_medal_tally', conn, if_exists='replace')


# Olympic Results
df = pd.read_csv('Olympic_Results.csv')

df.columns = df.columns.str.strip()

df.to_sql('results', conn, if_exists='replace')

# Olympics Country
df = pd.read_csv('Olympics_Country.csv')

df.columns = df.columns.str.strip()

df.to_sql('country', conn, if_exists='replace')


# Olympics Games
df = pd.read_csv('Olympics_Games.csv')

df.columns = df.columns.str.strip()

df.to_sql('games', conn, if_exists='replace')

# Відключення від БД
conn.close()