from sqlalchemy import create_engine, select, func, distinct, join, desc, cast, Float
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy import Column, Integer, Float, Text
import time

engine = create_engine('sqlite:///database_olympic.db')

Base = declarative_base()

# Визначення таблиць
class AthleteBio(Base):
    __tablename__ = 'athlete_bio'
    athlete_id = Column(Integer, primary_key=True)
    name = Column(Text)
    sex = Column(Text)
    born = Column(Text)
    height = Column(Float)
    weight = Column(Text)
    country = Column(Text)
    country_noc = Column(Text)
    description = Column(Text)
    special_notes = Column(Text)

class Results(Base):
    __tablename__ = 'results'
    result_id = Column(Integer, primary_key=True)
    event_title = Column(Text)
    edition = Column(Text)
    edition_id = Column(Integer)
    sport = Column(Text)
    sport_url = Column(Text)
    result_date = Column(Text)
    result_location = Column(Text)
    result_participants = Column(Text)
    result_format = Column(Text)
    result_detail = Column(Text)
    result_description = Column(Text)

class GamesMedalTally(Base):
    __tablename__ = 'games_medal_tally'
    edition_id = Column(Integer, primary_key=True)
    year = Column(Integer)
    country = Column(Text)
    country_noc = Column(Text)
    gold = Column(Integer)
    silver = Column(Integer)
    bronze = Column(Integer)
    total = Column(Integer)

class Games(Base):
    __tablename__ = 'games'
    edition_id = Column(Integer, primary_key=True)
    edition = Column(Text)
    year = Column(Integer)
    city = Column(Text)
    country_noc = Column(Text)
    start_date = Column(Text)
    end_date = Column(Text)
    competition_date = Column(Text)
    isHeld = Column(Text)

class Country(Base):
    __tablename__ = 'country'
    noc = Column(Text, primary_key=True)
    country = Column(Text)

class AthleteEventResults(Base):
    __tablename__ = 'athlete_event_results'
    result_id = Column(Integer, primary_key=True)
    edition_id = Column(Integer)
    country_noc = Column(Text)
    sport = Column(Text)
    event = Column(Text)
    athlete_id = Column(Integer)
    pos = Column(Text)
    medal = Column(Text)
    isTeamSport = Column(Integer)

# Запити у вигляді функцій
def query_1(session):
    stmt = select(
        Games.year,
        Country.country,
        AthleteBio.name.label('athlete_name'),
        AthleteEventResults.sport,
        AthleteEventResults.event,
        AthleteEventResults.medal,
        func.count(AthleteEventResults.medal).label('medal_count')
    ).select_from(
        join(Games, GamesMedalTally, Games.edition_id == GamesMedalTally.edition_id)
        .join(Country, GamesMedalTally.country_noc == Country.noc)
        .join(AthleteEventResults, 
              (Games.edition_id == AthleteEventResults.edition_id) & 
              (Country.noc == AthleteEventResults.country_noc))
        .join(AthleteBio, AthleteEventResults.athlete_id == AthleteBio.athlete_id)
    ).group_by(
        Games.year, Country.country, AthleteBio.name, 
        AthleteEventResults.sport, AthleteEventResults.event, AthleteEventResults.medal
    ).order_by(
        Games.year, Country.country, desc(func.count(AthleteEventResults.medal))
    )
    
    return session.execute(stmt).fetchall()

def query_2(session):
    stmt = select(
        Games.year,
        AthleteEventResults.sport,
        func.avg(AthleteBio.height).label('avg_height'),
        func.avg(cast(AthleteBio.weight, Float)).label('avg_weight')
    ).select_from(
        join(Games, AthleteEventResults, Games.edition_id == AthleteEventResults.edition_id)
        .join(AthleteBio, AthleteEventResults.athlete_id == AthleteBio.athlete_id)
    ).where(
        AthleteEventResults.medal.isnot(None)
    ).group_by(
        Games.year, AthleteEventResults.sport
    ).order_by(
        Games.year, AthleteEventResults.sport
    )
    
    return session.execute(stmt).fetchall()

def query_3(session):
    stmt = select(
        AthleteBio.name,
        func.count(distinct(Games.edition_id)).label('num_of_games'),
        func.group_concat(distinct(Games.year)).label('years')
    ).select_from(
        join(AthleteBio, AthleteEventResults, AthleteBio.athlete_id == AthleteEventResults.athlete_id)
        .join(Games, AthleteEventResults.edition_id == Games.edition_id)
    ).group_by(
        AthleteBio.name
    ).having(
        func.count(distinct(Games.edition_id)) > 1
    ).order_by(
        desc('num_of_games'), AthleteBio.name
    )
    
    return session.execute(stmt).fetchall()


queries = [query_1, query_2, query_3]
# Вимірювання часу
execution_times = []

# Виконання запитів за допомогою сесії
with Session(engine) as session:
    # цикл де і - номер запиту, а query - кожен запит по черзі
    for i, query_func in enumerate(queries):
        print(f'\n Запит {i + 1}:')
        
        # Таймер для вимірювання часу
        start_time = time.time()
        results = query_func(session)
        end_time = time.time()
        
        execution_time = end_time - start_time
        execution_times.append(execution_time)
        
        # Виведення бажаних результатів (5, щоб швидше бочити результати(час однаковий))
        for row in results[:5]:
            print(row)
        
        print(f'Час виконання запиту {i + 1}: {execution_time:.1f} секунд')

session.close()

extsum = 0
for i in execution_times:
    extsum += i
print(f'Загальний час виконання: {extsum:.1f} секунд')