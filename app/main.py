import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from database import get_db, init_database


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        if init_database():
            logger.info("Database initialized successfully")
        else:
            logger.warning(
                f"Database initialization failed - "
                f"API will start without database"
            )

    except Exception as e:
        logger.error(f"Database initialization error: {e}")

    yield


app = FastAPI(
    title="Dubai Population API",
    description="API for Dubai population data",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/")
async def root():
    return {"message": "UAE Population API is running"}


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        with get_db() as db:
            db.execute(text("SELECT 1"))
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
        }


@app.get("/api/emirate")
async def get_emirate_data(
    years: Optional[List[int]] = Query(None, description="Filter by years")
):
    """Get population data for entire Dubai emirate"""
    
    try:
        with get_db() as db:
            if years:
                result = db.execute(
                    text("""
                        SELECT * FROM analytics.emirate_population 
                        WHERE year = ANY(:years)
                        ORDER BY year
                    """),
                    {"years": years}
                )
            else:
                result = db.execute(
                    text("SELECT * FROM analytics.emirate_population ORDER BY year")
                )
            
            return [dict(row) for row in result.mappings()]
            
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error querying emirate data: {e}"
        )


@app.get("/api/communities")
async def get_all_communities(
    db: Session = Depends(get_db)
):
    """Get all communities with their sector information"""
    
    try:
        with get_db() as db:
            result = db.execute(
                text("""
                    SELECT 
                        c.code as community_code,
                        c.name_ar as community_name_ar,
                        c.name_en as community_name_en,
                        s.name_ar as sector_name_ar,
                        s.name_en as sector_name_en,
                        c.area_km2,
                        c.created_at,
                        c.updated_at
                    FROM dubai.communities c
                    LEFT JOIN dubai.sectors s ON c.sector_id = s.id
                    ORDER BY c.code
                """)
            )
            
            communities = [dict(row) for row in result.mappings()]
            
            if not communities:
                raise HTTPException(
                    status_code=404,
                    detail="No communities found"
                )
            
            return communities
            
    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error querying communities: {e}"
        )


@app.get("/api/sectors")
async def get_all_sectors(
    db: Session = Depends(get_db)
):
    """Get all sectors with summary information"""
    
    try:
        with get_db() as db:
            result = db.execute(
                text("""
                    SELECT 
                        s.id,
                        s.name_ar,
                        s.name_en,
                        s.total_population,
                        s.total_area,
                        s.total_density,
                        COUNT(c.id) as communities_count,
                        s.created_at,
                        s.updated_at
                    FROM dubai.sectors s
                    LEFT JOIN dubai.communities c ON s.id = c.sector_id
                    GROUP BY s.id
                    ORDER BY s.name_en
                """)
            )
            
            sectors = [dict(row) for row in result.mappings()]
            
            if not sectors:
                raise HTTPException(
                    status_code=404,
                    detail="No sectors found"
                )
            
            return sectors
            
    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error querying sectors: {e}"
        )


@app.get("/api/sectors/{sector_name}")
async def get_sector_data(
    sector_name: str,
    years: Optional[List[int]] = Query(None)
):
    """Get population data for specific sector"""
    
    try:
        with get_db() as db:
            if years:
                result = db.execute(
                    text("""
                        SELECT * FROM analytics.sector_population 
                        WHERE sector_name = :sector_name
                        AND year = ANY(:years)
                        ORDER BY year
                    """),
                    {"sector_name": sector_name, "years": years}
                )
            else:
                result = db.execute(
                    text("""
                        SELECT * FROM analytics.sector_population 
                        WHERE sector_name = :sector_name
                        ORDER BY year
                    """),
                    {"sector_name": sector_name}
                )
            
            data = [dict(row) for row in result.mappings()]
            
            if not data:
                raise HTTPException(
                    status_code=404,
                    detail=f"Sector '{sector_name}' not found"
                )
            
            return data

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error querying sector data: {e}"
        )


@app.get("/api/communities/{community_code}")
async def get_community_data(
    community_code: str,
    years: Optional[List[int]] = Query(None)
):
    """Get population data for specific community"""
    
    try:
        with get_db() as db:
            if years:
                result = db.execute(
                    text("""
                        SELECT * FROM analytics.community_population 
                        WHERE community_code = :community_code
                        AND year = ANY(:years)
                        ORDER BY year
                    """),
                    {"community_code": community_code, "years": years}
                )
            else:
                result = db.execute(
                    text("""
                        SELECT * FROM analytics.community_population 
                        WHERE community_code = :community_code
                        ORDER BY year
                    """),
                    {"community_code": community_code}
                )
            
            data = [dict(row) for row in result.mappings()]
            
            if not data:
                raise HTTPException(
                    status_code=404,
                    detail=f"Community '{community_code}' not found"
                )
            
            return data

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error querying community data: {e}"
        )
