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
    
queries = [
    # Запит 1 "Медалі Олімпіад: спортсмени, країни, події"
    select(Games.year, Country.country, AthleteBio.name, AthleteEventResults.sport, AthleteEventResults.event, AthleteEventResults.medal, func.count(AthleteEventResults.medal))
    .join(AthleteEventResults, Games.edition_id == AthleteEventResults.edition_id)
    .join(AthleteBio, AthleteEventResults.athlete_id == AthleteBio.athlete_id)
    .join(Country, AthleteBio.country_noc == Country.noc)
    .group_by(Games.year, Country.country, AthleteBio.name, AthleteEventResults.sport, AthleteEventResults.event, AthleteEventResults.medal)
    .order_by(Games.year, Country.country, func.count(AthleteEventResults.medal).desc()),

    # Запит 2 "Середній зріст і вага медалістів за видами спорту"
    select(Games.year, AthleteEventResults.sport, func.avg(AthleteBio.height).label('avg_height'), func.avg(AthleteBio.weight).label('avg_weight'))
    .join(AthleteEventResults, Games.edition_id == AthleteEventResults.edition_id)
    .join(AthleteBio, AthleteEventResults.athlete_id == AthleteBio.athlete_id)
    .where(AthleteEventResults.medal.isnot(None))
    .group_by(Games.year, AthleteEventResults.sport)
    .order_by(Games.year, AthleteEventResults.sport),

    # Запит 3 "Топ багатоподієвих спортсменів із медалями"
    select(AthleteBio.name, func.count(AthleteEventResults.event).label('event_count'), func.count(AthleteEventResults.medal).label('medal_count'))
    .join(AthleteEventResults, AthleteBio.athlete_id == AthleteEventResults.athlete_id)
    .where(AthleteEventResults.medal.isnot(None))
    .group_by(AthleteBio.name)
    .having(func.count(AthleteEventResults.event) > 3)
    .order_by(func.count(AthleteEventResults.medal).desc(), func.count(AthleteEventResults.event).desc())
]