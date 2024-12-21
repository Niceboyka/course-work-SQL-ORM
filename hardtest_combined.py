import time
import sqlite3
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models_n_queries_sqlalchemy import queries, Base
from queries_sql import query1, query2, query3

# Підключення SQLite
sqlite_connection = sqlite3.connect("database_olympic.db")
sqlite_cursor = sqlite_connection.cursor()

# Підключення SQLAlchemy
engine = create_engine("sqlite:///database_olympic.db")
Session = sessionmaker(bind=engine)
session = Session()

# List-и для часу обох моделей
time_sql = []
time_sqlalchemy = []

# Функція для аналізу виконання та часу запитів
def execute_queries(sql_query, sqlalchemy_query):
    sqlite_times = []
    sqlalchemy_times = []

    for iteraction in range(10):
        # SQLite
        start_time = time.time()
        sqlite_cursor.execute(sql_query)
        sqlite_results = sqlite_cursor.fetchall()
        elapsed_sqlite = time.time() - start_time
        sqlite_times.append(elapsed_sqlite)
        print(f"SQLite: {len(sqlite_results)} данных, {elapsed_sqlite:.6f} секунд")

        # SQLAlchemy
        start_time = time.time()
        sqlalchemy_results = session.execute(sqlalchemy_query).fetchall()
        elapsed_sqlalchemy = time.time() - start_time
        sqlalchemy_times.append(elapsed_sqlalchemy)
        print(f"SQLAlchemy: {len(sqlalchemy_results)} данных, {elapsed_sqlalchemy:.6f} секунд")
    
    # Загальний час
    total_sqlite_time = sum(sqlite_times)
    total_sqlalchemy_time = sum(sqlalchemy_times)
    print(f"{total_sqlalchemy_time:.2f} - загальний час запиту на SQLAlchemy")
    print(f"{total_sqlite_time:.2f} - загальний час запиту на SQLite")

    # Обчислення відсотків
    for i in range(len(sqlite_times)):
        percent1 = ((sqlite_times[i] - sqlalchemy_times[i])/total_sqlalchemy_time)*100
        percent2 = ((sqlite_times[i] - sqlalchemy_times[i])/total_sqlite_time)*100
        if percent1 > 0 and percent2 > 0:
            print(f"Sqlite працює на {(percent1):.3f}% або {(percent2):.3f}% повільніше ніж SqlAlchemy")
        elif percent1 < 0 and percent2 < 0:
            print(f"SqlAlchemy працює на {abs(percent1):.3f}% або {abs(percent2):.3f}% повільніше ніж Sqlite")
            
    percent_sql = ((total_sqlite_time - total_sqlalchemy_time) / total_sqlalchemy_time)*100
    percent_sqlalchemy = ((total_sqlite_time - total_sqlalchemy_time) / total_sqlite_time)*100

    # Запис результатів
    time_sql.append(total_sqlite_time)
    time_sqlalchemy.append(total_sqlalchemy_time)

    # Виведення відсотків в термінал
    procent_querie = [f"{percent_sql:.2f}%", f"{percent_sqlalchemy:.2f}%"]
    print(f"Проценти: {[(procent_querie[i]) for i in range(len(procent_querie))]}")


# Звертання до функції надаючи по одному запиту обох моделей
execute_queries(query1, queries[0])  # Запит 1
execute_queries(query2, queries[1])  # Запит 2
execute_queries(query3, queries[2])  # Запит 3

# Закриття сесій
sqlite_connection.close()
session.close()
