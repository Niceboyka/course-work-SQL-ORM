# Запит 1 "Медалі Олімпіад: спортсмени, країни, події"
query1 = """
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
    """
# Запит 2 "Середній зріст і вага медалістів за видами спорту"
query2 = """
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
    """
# Запит 3 "Топ багатоподієвих спортсменів із медалями"
query3 = """
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