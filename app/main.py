from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db
from typing import List, Optional


app = FastAPI(title="Dubai Population API", version="1.0")


@app.get("/")
async def root():
    return {
        "message": "Dubai Population API - Check /docs for endpoints"
    }


@app.get("/population/emirate")
async def get_emirate_population(
    years: Optional[List[int]] = Query(None),
    db: Session = Depends(get_db)
):
    query = """
    SELECT year, SUM(population) as total_population, 
           ROUND(SUM(population) / SUM(area_km2), 2) as density
    FROM population_data pd
    JOIN communities c ON pd.community_id = c.id
    GROUP BY year
    """
    if years:
        query += f" WHERE year IN ({','.join(map(str, years))})"
    query += " ORDER BY year"
    
    result = db.execute(text(query))
    return [dict(row) for row in result.mappings()]


@app.get("/population/sector/{sector_name}")
async def get_sector_population(
    sector_name: str,
    years: Optional[List[int]] = Query(None),
    db: Session = Depends(get_db)
):
    query = """
    SELECT s.name as sector_name, pd.year, 
           SUM(pd.population) as total_population,
           ROUND(SUM(pd.population) / SUM(c.area_km2), 2) as density
    FROM population_data pd
    JOIN communities c ON pd.community_id = c.id
    JOIN sectors s ON c.sector_id = s.id
    WHERE s.name = :sector_name
    """
    if years:
        query += f" AND pd.year IN ({','.join(map(str, years))})"
    query += " GROUP BY s.name, pd.year ORDER BY pd.year"
    
    result = db.execute(text(query), {"sector_name": sector_name})
    return [dict(row) for row in result.mappings()]


@app.get("/population/community/{community_name}")
async def get_community_population(
    community_name: str,
    years: Optional[List[int]] = Query(None),
    db: Session = Depends(get_db)
):
    query = """
    SELECT c.name as community_name, s.name as sector_name, 
           pd.year, pd.population, 
           ROUND(pd.population / c.area_km2, 2) as density,
           pd.is_estimated
    FROM population_data pd
    JOIN communities c ON pd.community_id = c.id
    JOIN sectors s ON c.sector_id = s.id
    WHERE c.name = :community_name
    """
    if years:
        query += f" AND pd.year IN ({','.join(map(str, years))})"
    query += " ORDER BY pd.year"
    
    result = db.execute(text(query), {"community_name": community_name})
    return [dict(row) for row in result.mappings()]


@app.get("/anomalies")
async def get_anomalies(
    threshold: float = 1.5,
    db: Session = Depends(get_db)
):
    query = """
    WITH growth_rates AS (
        SELECT 
            c.name as community,
            pd.year,
            pd.population,
            LAG(pd.population) OVER (PARTITION BY c.id ORDER BY pd.year) as prev_population,
            (pd.population - LAG(pd.population) OVER (PARTITION BY c.id ORDER BY pd.year)) * 100.0 / 
            NULLIF(LAG(pd.population) OVER (PARTITION BY c.id ORDER BY pd.year), 0) as growth_rate
        FROM population_data pd
        JOIN communities c ON pd.community_id = c.id
    )
    SELECT community, year, population, growth_rate
    FROM growth_rates
    WHERE growth_rate IS NOT NULL AND ABS(growth_rate) > :threshold
    ORDER BY ABS(growth_rate) DESC
    """
    
    result = db.execute(text(query), {"threshold": threshold})
    return [dict(row) for row in result.mappings()]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
