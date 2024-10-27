import sqlite3
import time
import asyncio

# Підключення до бази даних
db_path = 'database_olympic.db'

# Запити
queries = [
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
        athlete_event_results aer ON g.edition_id = aer.edition_id
    JOIN 
        athlete_bio ab ON aer.athlete_id = ab.athlete_id
    JOIN 
        country c ON ab.country_noc = c.noc
    GROUP BY 
        g.year, c.country, ab.name, aer.sport, aer.event, aer.medal
    ORDER BY 
        g.year, c.country, COUNT(aer.medal) DESC;
    """,
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
    """
    SELECT 
    ab.name, 
    COUNT(aer.event) AS event_count, 
    COUNT(aer.medal) AS medal_count
FROM 
    athlete_bio ab
JOIN 
    athlete_event_results aer ON ab.athlete_id = aer.athlete_id
WHERE 
    aer.medal IS NOT NULL
GROUP BY 
    ab.name
HAVING 
    COUNT(aer.event) > 3
ORDER BY 
    medal_count DESC, event_count DESC;
    """
]

# Функція для виконання запиту і засічення часу
async def execute_query(query, conn):
    cursor = conn.cursor()
    start_time = time.time()
    cursor.execute(query)
    result = cursor.fetchall()
    execution_time = time.time() - start_time
    return execution_time, result

# Основна функція для запуску циклічного виконання запитів
async def run_queries():
    conn = sqlite3.connect(db_path)  # Підключення до бази SQLite
    
    total_time = 0  # Загальний час виконання всіх ітерацій
    iterations = 10  # Кількість ітерацій

    # Засікаємо час виконання всієї програми
    start_program_time = time.time()
    
    # Список для збереження часу ітерацій
    iteration_times = []

    # Список для збереження часу кожного запиту в кожній ітерації
    query_times_per_iteration = [[] for _ in range(len(queries))]

    for i in range(iterations):
        print(f"Ітерація {i + 1}...")
        iteration_time = 0  # Час для поточної ітерації

        # Виконання запитів і засічення часу
        for idx, query in enumerate(queries):
            exec_time, result = await execute_query(query, conn)
            query_times_per_iteration[idx].append(exec_time)  # Зберігаємо час виконання запиту
            iteration_time += exec_time
            print(f"Запит {idx + 1}, час виконання: {exec_time:.1f} секунд, кількість результатів: {len(result)}")

        print(f"Час виконання ітерації {i + 1}: {iteration_time:.1f} секунд\n")

    # Для збереження загальної різниці часу і відсотків для всіх запитів
    total_diff_time_all_queries = 0
    total_percentage_diff_all_queries = 0
    total_comparisons = 0

    # Порівнюємо час між ітераціями для кожного запиту
    for idx in range(len(queries)):
        print(f"\nПорівняння запиту {idx + 1} між ітераціями:")
        total_diff_time = 0
        total_percentage_diff = 0
        comparisons = 0

        for i in range(1, len(query_times_per_iteration[idx])):
            prev_time = query_times_per_iteration[idx][i - 1]
            curr_time = query_times_per_iteration[idx][i]
            time_diff = curr_time - prev_time

            if time_diff >= 0:
                percentage_diff = (time_diff / prev_time) * 100
            else:
                percentage_diff = (1 - (curr_time / prev_time)) * 100

            total_diff_time += abs(time_diff)
            total_percentage_diff += abs(percentage_diff)
            comparisons += 1

            print(f"Ітерація {i} до {i + 1}: різниця в часі: {abs(time_diff):.1f} секунд ({abs(percentage_diff):.2f}%)")

        # Додаємо до загальних підрахунків
        total_diff_time_all_queries += total_diff_time
        total_percentage_diff_all_queries += total_percentage_diff
        total_comparisons += comparisons

        # Підрахунок середніх різниць для кожного запиту
        if comparisons > 0:
            avg_diff_time = total_diff_time / comparisons
            avg_percentage_diff = total_percentage_diff / comparisons
            print(f"Середня різниця для запиту {idx + 1}: {avg_diff_time:.4f} секунд ({avg_percentage_diff:.2f}%)")

    # Підрахунок загальних середніх значень
    if total_comparisons > 0:
        avg_total_diff_time = total_diff_time_all_queries / total_comparisons
        avg_total_percentage_diff = total_percentage_diff_all_queries / total_comparisons
        print(f"\nСередня різниця по всіх запитах: {avg_total_diff_time:.4f} секунд ({avg_total_percentage_diff:.2f}%)")

    iteration_times.append(iteration_time)

    # Підраховуємо середній час однієї ітерації (сума трьох запитів)
    total_iteration_time = sum(iteration_times)
    average_iteration_time = total_iteration_time / len(iteration_times)

    print(f"Середній час виконання однієї ітерації (3 запити): {average_iteration_time:.1f} секунд")
    
    # Підрахуємо загальний час виконання програми
    total_program_time = time.time() - start_program_time
    print(f"\nЗагальний час виконання програми: {total_program_time:.1f} секунд")

    conn.close()  # Закриття підключення до бази

# Запуск асинхронної програми
asyncio.run(run_queries())