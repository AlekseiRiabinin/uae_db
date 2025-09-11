import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from typing import Dict


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class DubaiDataLoader:
    def __init__(self, db_url=None):

        if db_url:
            self.db_url = db_url

        else:
            db_host = os.getenv('POSTGRES_HOST', 'postgres')
            db_port = os.getenv('POSTGRES_PORT', '5432')
            db_name = os.getenv('POSTGRES_DB', 'dubai_population')
            db_user = os.getenv('POSTGRES_USER', 'postgres')
            db_password = os.getenv('POSTGRES_PASSWORD', 'postgres')
            
            self.db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        self.engine = create_engine(self.db_url)
        self.data_dir = '/app/data'
        
    def check_csv_files(self):
        """Check if all required CSV files exist"""

        required_files = ['sectors.csv', 'communities.csv', 'population.csv']
        missing_files = []
        
        for filename in required_files:
            filepath = os.path.join(self.data_dir, filename)

            if not os.path.exists(filepath):
                missing_files.append(filename)
                logger.error(f"Missing file: {filename}")

            else:
                logger.info(f"Found file: {filename}")
        
        if missing_files:
            raise FileNotFoundError(
                f"Missing required CSV files: "
                f"{', '.join(missing_files)}"
            )
        
        return True
    
    def load_sectors(self) -> Dict[str, int]:
        """Load sectors data and return mapping of sector_name_en to sector_id"""

        logger.info("Loading sectors data...")
        
        sectors_file = os.path.join(self.data_dir, 'sectors.csv')
        sectors_df = pd.read_csv(sectors_file, encoding='utf-8-sig')
        
        sectors_df = sectors_df.dropna(
            subset=['sector_name_en', 'total_population']
        )
        sectors_df = (
            sectors_df[~sectors_df['sector_name_en'].str.contains(
                'Sector & Community|Distribution', na=False
            )]
        )
        
        sector_mapping = {}
        
        with self.engine.connect() as conn:
            table_exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'dubai' 
                    AND table_name = 'sectors'
                )
            """)).scalar()
            
            if table_exists:
                conn.execute(text("DELETE FROM dubai.sectors"))
            conn.commit()
            
            for _, row in sectors_df.iterrows():
                result = conn.execute(text("""
                    INSERT INTO dubai.sectors (name_ar, name_en, total_population, total_area, total_density)
                    VALUES (:name_ar, :name_en, :total_population, :total_area, :total_density)
                    RETURNING id
                """), {
                    'name_ar': row['sector_name_ar'],
                    'name_en': row['sector_name_en'],
                    'total_population': row['total_population'],
                    'total_area': row['total_area'],
                    'total_density': row['total_density']
                })
                
                sector_id = result.scalar()
                sector_mapping[row['sector_name_en']] = sector_id
                logger.debug(
                    f"Loaded sector: "
                    f"{row['sector_name_en']} -> ID: {sector_id}"
                )
            
            conn.commit()
        
        logger.info(
            f"Sectors data loaded successfully. "
            f"Loaded {len(sector_mapping)} sectors."
        )
        return sector_mapping
    
    def load_communities(
        self, sector_mapping: Dict[str, int]
    ) -> Dict[str, int]:
        """Load communities data and return mapping of community_code to community_id"""

        logger.info("Loading communities data...")
        
        communities_file = os.path.join(self.data_dir, 'communities.csv')
        communities_df = pd.read_csv(communities_file, encoding='utf-8-sig')
        
        # Clean the data
        communities_df = (
            communities_df.dropna(
                subset=['community_code', 'name_en', 'sector_name_en']
            )
        )
        communities_df = (
            communities_df[communities_df['sector_name_en'] != 'Sector & Community']
        )
        
        community_mapping = {}
        
        with self.engine.connect() as conn:
            table_exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'dubai' 
                    AND table_name = 'communities'
                )
            """)).scalar()
            
            if table_exists:
                conn.execute(text("DELETE FROM dubai.communities"))
            conn.commit()
            
            for _, row in communities_df.iterrows():
                sector_id = sector_mapping.get(row['sector_name_en'])
                if not sector_id:
                    logger.warning(
                        f"Sector '{row['sector_name_en']}' "
                        f"not found for community "
                        f"{row['community_code']}"
                    )
                    continue
                
                result = conn.execute(text("""
                    INSERT INTO dubai.communities (code, name_ar, name_en, sector_id, area_km2)
                    VALUES (:code, :name_ar, :name_en, :sector_id, :area_km2)
                    RETURNING id
                """), {
                    'code': row['community_code'],
                    'name_ar': row['name_ar'],
                    'name_en': row['name_en'],
                    'sector_id': sector_id,
                    'area_km2': row['area_km2']
                })
                
                community_id = result.scalar()
                community_mapping[row['community_code']] = community_id
                logger.debug(
                    f"Loaded community: "
                    f"{row['community_code']} -> ID: {community_id}"
                )
            
            conn.commit()
        
        logger.info(
            f"Communities data loaded successfully. "
            f"Loaded {len(community_mapping)} communities."
        )
        return community_mapping
    
    def load_population_data(self, community_mapping: Dict[str, int]):
        """Load population data into PostgreSQL"""

        logger.info("Loading population data...")
        
        population_file = os.path.join(self.data_dir, 'population.csv')
        population_df = pd.read_csv(population_file, encoding='utf-8-sig')
        
        population_df = population_df.dropna(
            subset=['community_code', 'year', 'population', 'density']
        )
        
        successful_records = 0
        total_records = len(population_df)
        
        with self.engine.connect() as conn:
            table_exists = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'dubai' 
                    AND table_name = 'population_data'
                )
            """)).scalar()
            
            if table_exists:
                conn.execute(text("DELETE FROM dubai.population_data"))
            conn.commit()
            
            batch_size = 500
            
            for i in range(0, total_records, batch_size):
                batch = population_df.iloc[i:i + batch_size]
                batch_success = 0
                
                for _, row in batch.iterrows():
                    community_id = community_mapping.get(
                        row['community_code']
                    )
                    if not community_id:
                        logger.debug(
                            f"Community '{row['community_code']}' "
                            f"not found, skipping population data"
                        )
                        continue
                    
                    try:
                        conn.execute(text("""
                            INSERT INTO dubai.population_data (community_id, year, population, density, is_estimated, is_anomaly)
                            VALUES (:community_id, :year, :population, :density, :is_estimated, :is_anomaly)
                        """), {
                            'community_id': community_id,
                            'year': row['year'],
                            'population': row['population'],
                            'density': row['density'],
                            'is_estimated': row['is_estimated'],
                            'is_anomaly': row.get('is_anomaly', False)
                        })
                        successful_records += 1
                        batch_success += 1
                    except Exception as e:
                        logger.warning(
                            f"Failed to insert population data for community "
                            f"{row['community_code']}, year {row['year']}: {e}"
                        )
                
                logger.info(
                    f"Processed batch: "
                    f"{min(i + batch_size, total_records)}/{total_records} "
                    f"records ({batch_success} successful)"
                )
            
            conn.commit()
        
        logger.info(
            f"Population data loaded successfully. "
            f"Loaded {successful_records} records."
        )
        return successful_records
    
    def create_indexes(self):
        """Create indexes for better performance"""

        logger.info("Creating indexes...")
        
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_population_community ON dubai.population_data(community_id)",
            "CREATE INDEX IF NOT EXISTS idx_population_year ON dubai.population_data(year)",
            "CREATE INDEX IF NOT EXISTS idx_communities_sector ON dubai.communities(sector_id)",
            "CREATE INDEX IF NOT EXISTS idx_communities_code ON dubai.communities(code)"
        ]
        
        with self.engine.connect() as conn:
            for index_sql in indexes:
                try:
                    conn.execute(text(index_sql))
                    logger.debug(f"Created index: {index_sql}")
                except Exception as e:
                    logger.warning(f"Failed to create index: {e}")
            
            conn.commit()
        
        logger.info("Indexes created successfully")
    
    def verify_data_loading(self):
        """Verify that data was loaded correctly"""

        logger.info("Verifying data loading...")
        
        with self.engine.connect() as conn:

            sectors_count = conn.execute(
                text("SELECT COUNT(*) FROM dubai.sectors")
            ).scalar()

            communities_count = conn.execute(
                text("SELECT COUNT(*) FROM dubai.communities")
            ).scalar()

            population_count = conn.execute(
                text("SELECT COUNT(*) FROM dubai.population_data")
            ).scalar()
            
            year_counts_result = conn.execute(
                text("SELECT year, COUNT(*) FROM dubai.population_data GROUP BY year ORDER BY year")
            )
            year_stats = {row[0]: row[1] for row in year_counts_result}
        
        logger.info(f"Verification results:")
        logger.info(f"  - Sectors: {sectors_count}")
        logger.info(f"  - Communities: {communities_count}")
        logger.info(f"  - Population records: {population_count}")
        logger.info(f"  - Years covered: {list(year_stats.keys())}")
        
        return {
            'sectors': sectors_count,
            'communities': communities_count,
            'population_records': population_count,
            'year_distribution': year_stats
        }

    def load_all_data(self):
        """Load all data into PostgreSQL in correct order"""

        try:
            logger.info("Starting Dubai population data loading...")
            logger.info(f"Database URL: {self.db_url}")
            
            self.check_csv_files()
            
            sector_mapping = self.load_sectors()
            community_mapping = self.load_communities(sector_mapping)
            population_count = self.load_population_data(community_mapping)
            
            self.create_indexes()
            
            verification = self.verify_data_loading()
            
            logger.info("=" * 50)
            logger.info("DATA LOADING COMPLETED SUCCESSFULLY!")
            logger.info("=" * 50)
            logger.info(f"Sectors loaded: {verification['sectors']}")
            logger.info(f"Communities loaded: {verification['communities']}")
            logger.info(
                f"Population records loaded: "
                f"{verification['population_records']} "
                f"(attempted: {population_count})"
            )
            logger.info(
                f"Years covered: "
                f"{list(verification['year_distribution'].keys())}"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

def main():
    """Main function to run the data loader"""

    try:
        loader = DubaiDataLoader()
        
        success = loader.load_all_data()
        
        if success:
            logger.info("🎉 Data loading completed successfully!")
            return True
        else:
            logger.error("Data loading failed")
            return False
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
