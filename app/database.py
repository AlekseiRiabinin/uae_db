import os
import time
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager


logger = logging.getLogger(__name__)


# SQLAlchemy base class for models
Base = declarative_base()

def get_database_url():
    """Get database URL from environment variables with fallback"""
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        logger.info("Using DATABASE_URL from environment")
        return database_url
    
    db_host = os.getenv("POSTGRES_HOST", "postgres")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "dubai_population")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    
    url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    logger.info(f"Using constructed database URL: {url}")
    return url


def create_engine_with_retry(max_retries=10, delay=3):
    """Create database engine with retry logic"""
    for attempt in range(max_retries):
        try:
            DATABASE_URL = get_database_url()
            engine = create_engine(
                DATABASE_URL,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                echo=False
            )
            
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Database connection established successfully")
            return engine
            
        except SQLAlchemyError as e:
            logger.warning(
                f"Database connection attempt "
                f"{attempt + 1}/{max_retries} failed: {e}"
            )
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error("All database connection attempts failed")
                raise


_engine = None
_SessionLocal = None


def init_database():
    """Initialize database connection"""

    global _engine, _SessionLocal

    try:
        _engine = create_engine_with_retry()
        _SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=_engine
        )
        logger.info("Database initialized successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return False


def get_engine():
    """Get database engine (lazy initialization)"""

    global _engine

    if _engine is None:
        init_database()
    return _engine


def get_session_local():
    """Get sessionmaker (lazy initialization)"""

    global _SessionLocal

    if _SessionLocal is None:
        init_database()
    return _SessionLocal


@contextmanager
def get_db():
    """Context manager for database sessions"""

    if _SessionLocal is None:
        init_database()
    
    if _SessionLocal is None:
        raise Exception("Database not initialized")
    
    db = _SessionLocal()

    try:
        yield db
        db.commit()

    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Database session error: {e}")
        raise

    finally:
        db.close()
