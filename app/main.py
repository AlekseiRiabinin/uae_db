from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from .database import get_db


app = FastAPI(
    title="Dubai Population Analytics API",
    description="Senior Data Engineer Technical Assessment - Dubai Population Data",
    version="1.0.0"
)


@app.get("/")
async def root():
    return {
        "message": "Dubai Population Analytics API",
        "endpoints": {
            "emirate_data": "/api/emirate",
            "sector_data": "/api/sectors/{sector_name}",
            "community_data": "/api/communities/{community_code}",
            "anomalies": "/api/anomalies"
        }
    }


@app.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """Health check endpoint for Docker healthcheck"""

    try:
        db.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}

    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Database connection failed: {e}"
        )


@app.get("/api/emirate")
async def get_emirate_data(
    years: Optional[List[int]] = Query(None, description="Filter by years"),
    db: Session = Depends(get_db)
):
    """Get population data for entire Dubai emirate"""

    query = "SELECT * FROM analytics.emirate_population"
    if years:
        query += f" WHERE year IN ({','.join(map(str, years))})"
    query += " ORDER BY year"
    
    result = db.execute(text(query))
    return [dict(row) for row in result.mappings()]


@app.get("/api/sectors/{sector_name}")
async def get_sector_data(
    sector_name: str,
    years: Optional[List[int]] = Query(None),
    db: Session = Depends(get_db)
):
    """Get population data for specific sector"""

    query = """
    SELECT * FROM analytics.sector_population 
    WHERE sector_name_en = :sector_name
    """
    if years:
        query += f" AND year IN ({','.join(map(str, years))})"
    query += " ORDER BY year"
    
    result = db.execute(text(query), {"sector_name": sector_name})
    data = [dict(row) for row in result.mappings()]
    
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"Sector '{sector_name}' not found"
        )
    
    return data


@app.get("/api/communities/{community_code}")
async def get_community_data(
    community_code: str,
    years: Optional[List[int]] = Query(None),
    db: Session = Depends(get_db)
):
    """Get population data for specific community"""

    query = """
    SELECT * FROM analytics.community_population 
    WHERE community_code = :community_code
    """
    if years:
        query += f" AND year IN ({','.join(map(str, years))})"
    query += " ORDER BY year"
    
    result = db.execute(text(query), {"community_code": community_code})
    data = [dict(row) for row in result.mappings()]
    
    if not data:
        raise HTTPException(status_code=404, detail=f"Community '{community_code}' not found")
    
    return data


@app.get("/api/anomalies")
async def get_anomalies(
    severity_threshold: float = Query(0.7, ge=0.0, le=1.0),
    resolved: bool = Query(False),
    db: Session = Depends(get_db)
):
    """Get detected population anomalies"""

    query = """
    SELECT * FROM analytics.population_anomalies_report 
    WHERE severity_score >= :threshold AND is_resolved = :resolved
    ORDER BY detected_at DESC
    """
    
    result = db.execute(text(query), {
        "threshold": severity_threshold,
        "resolved": resolved
    })
    
    return [dict(row) for row in result.mappings()]
