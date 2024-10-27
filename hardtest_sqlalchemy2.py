import asyncio
import time
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Float, select, func

# Створення базового класу для моделей
Base = declarative_base()

# Моделі таблиць
class Games(Base):
    __tablename__ = 'games'
    edition_id = Column(Integer, primary_key=True)
    year = Column(Integer)
    season = Column(String)
    city = Column(String)

class Country(Base):
    __tablename__ = 'country'
    noc = Column(String, primary_key=True)
    country = Column(String)

class AthleteBio(Base):
    __tablename__ = 'athlete_bio'
    athlete_id = Column(Integer, primary_key=True)
    name = Column(String)
    gender = Column(String)
    height = Column(Float)
    weight = Column(Float)
    country_noc = Column(String)

class AthleteEventResults(Base):
    __tablename__ = 'athlete_event_results'
    event_id = Column(Integer, primary_key=True)
    athlete_id = Column(Integer)
    edition_id = Column(Integer)
    sport = Column(String)
    event = Column(String)
    medal = Column(String)

# Налаштування асинхронного двигуна
DATABASE_URL = "sqlite+aiosqlite:///database_olympic.db"
engine = create_async_engine(DATABASE_URL, echo=False)

# Створення асинхронної сесії
async_session = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Функція для виконання запиту і засічення часу
async def execute_query(query, session):
    start_time = time.time()
    result = await session.execute(query)
    execution_time = time.time() - start_time
    return execution_time, result.fetchall()

# Основна функція для запуску циклічного виконання запитів
async def run_queries():
    async with async_session() as session:
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
                exec_time, result = await execute_query(query, session)
                query_times_per_iteration[idx].append(exec_time)  # Зберігаємо час виконання запиту
                iteration_time += exec_time
                print(f"Запит {idx + 1}, час виконання: {exec_time:.1f} секунд, кількість результатів: {len(result)}")

            # Додаємо час поточної ітерації до загального списку
            iteration_times.append(iteration_time)
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

        # Підраховуємо середній час однієї ітерації (сума трьох запитів)
        total_iteration_time = sum(iteration_times)
        average_iteration_time = total_iteration_time / len(iteration_times)

        print(f"Середній час виконання однієї ітерації (3 запити): {average_iteration_time:.1f} секунд")

        # Підрахуємо загальний час виконання програми
        total_program_time = time.time() - start_program_time
        print(f"\nЗагальний час виконання програми: {total_program_time:.1f} секунд")

# Запити, взяті з попереднього коду
queries = [
    # Запит 1
    select(Games.year, Country.country, AthleteBio.name, AthleteEventResults.sport, AthleteEventResults.event, AthleteEventResults.medal, func.count(AthleteEventResults.medal))
    .join(AthleteEventResults, Games.edition_id == AthleteEventResults.edition_id)
    .join(AthleteBio, AthleteEventResults.athlete_id == AthleteBio.athlete_id)
    .join(Country, AthleteBio.country_noc == Country.noc)
    .group_by(Games.year, Country.country, AthleteBio.name, AthleteEventResults.sport, AthleteEventResults.event, AthleteEventResults.medal)
    .order_by(Games.year, Country.country, func.count(AthleteEventResults.medal).desc()),

    # Запит 2
    select(Games.year, AthleteEventResults.sport, func.avg(AthleteBio.height).label('avg_height'), func.avg(AthleteBio.weight).label('avg_weight'))
    .join(AthleteEventResults, Games.edition_id == AthleteEventResults.edition_id)
    .join(AthleteBio, AthleteEventResults.athlete_id == AthleteBio.athlete_id)
    .where(AthleteEventResults.medal.isnot(None))
    .group_by(Games.year, AthleteEventResults.sport)
    .order_by(Games.year, AthleteEventResults.sport),

    # Запит 3
    select(AthleteBio.name, func.count(AthleteEventResults.event).label('event_count'), func.count(AthleteEventResults.medal).label('medal_count'))
    .join(AthleteEventResults, AthleteBio.athlete_id == AthleteEventResults.athlete_id)
    .where(AthleteEventResults.medal.isnot(None))
    .group_by(AthleteBio.name)
    .having(func.count(AthleteEventResults.event) > 3)
    .order_by(func.count(AthleteEventResults.medal).desc(), func.count(AthleteEventResults.event).desc())
]

# Запуск асинхронної програми
asyncio.run(run_queries())
