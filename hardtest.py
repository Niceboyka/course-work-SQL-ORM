import sqlite3
import time

conn = sqlite3.connect('database_olympic.db')
cursor = conn.cursor()

# Запити
queries = [
    # Запит 1: Повний медальний залік по всіх країнах з деталізацією спортсменів, кожного Олімпійського року
    """
    SELECT 
        g.year, 
        c.country, 
        ab.name AS athlete_name, 
        aer.sport, 
        aer.event, 
        aer.medal, 
        COUNT(aer.medal) AS medal_count
    FROM 
        games g
    JOIN 
        games_medal_tally gmt ON g.edition_id = gmt.edition_id
    JOIN 
        country c ON gmt.country_noc = c.noc
    JOIN 
        athlete_event_results aer ON g.edition_id = aer.edition_id AND c.noc = aer.country_noc
    JOIN 
        athlete_bio ab ON aer.athlete_id = ab.athlete_id
    GROUP BY 
        g.year, c.country, ab.name, aer.sport, aer.event, aer.medal
    ORDER BY 
        g.year, c.country, COUNT(aer.medal) DESC;
    """,

    # Запит 2: Середнє зростання і вага спортсменів, які виграли медалі, з кожного виду спорту та року
    """
    SELECT 
        g.year, 
        aer.sport, 
        AVG(ab.height) AS avg_height, 
        AVG(ab.weight) AS avg_weight
    FROM 
        games g
    JOIN 
        athlete_event_results aer ON g.edition_id = aer.edition_id
    JOIN 
        athlete_bio ab ON aer.athlete_id = ab.athlete_id
    WHERE 
        aer.medal IS NOT NULL
    GROUP BY 
        g.year, aer.sport
    ORDER BY 
        g.year, aer.sport;
    """,

    # Запит 3: Спортсмени, які брали участь у найбільшій кількості Олімпійських ігор, з деталізацією з кожної гри
    """
    SELECT 
        ab.name, 
        COUNT(DISTINCT g.edition_id) AS num_of_games, 
        GROUP_CONCAT(DISTINCT g.year) AS years
    FROM 
        athlete_bio ab
    JOIN 
        athlete_event_results aer ON ab.athlete_id = aer.athlete_id
    JOIN 
        games g ON aer.edition_id = g.edition_id
    GROUP BY 
        ab.name
    HAVING 
        COUNT(DISTINCT g.edition_id) > 1
    ORDER BY 
        num_of_games DESC, ab.name;
    """
]

# Вимірювання часу
execution_times = []

# цикл де і - номер запиту, а query - кожен запит по черзі
for i, query in enumerate(queries):
    print(f'\n Запит {i + 1}:')
    
    # Таймер для вимірювання часу
    start_time = time.time()
    cursor.execute(query)
    results = cursor.fetchall()
    end_time = time.time()
    
    # Вимірювання часу запиту
    execution_time = end_time - start_time
    execution_times.append(execution_time)
    
    # Виведення бажаних результатів (5, щоб швидше бочити результати(час однаковий))
    for row in results[:5]:
        print(row)
    
    print(f'Час виконання запиту {i + 1}: {execution_time:.1f} секунд')

conn.close()

extsum = 0
for i in execution_times:
    extsum += i
print(f'Загальний час виконання: {extsum:.1f} секунд')