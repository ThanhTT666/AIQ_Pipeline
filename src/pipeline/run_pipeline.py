
import sys
from pathlib import Path

# Add parent directory to path to allow imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.ingestion.load_data import load_all_csv
from src.processing.clean_data import clean_data
from src.processing.transform_data import create_analytics
from src.processing.parquet_lake import export_to_parquet_lake
from src.processing.duckdb_warehouse import create_warehouse, load_data_to_warehouse
from src.quality.data_quality import validate_data, save_quality_report
from src.quality.logger import setup_logging

def run():
    # Initialize logging
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("Starting AIQ Pipeline")
    logger.info("=" * 60)
    
    try:
        # Phase 1: Load raw data
        logger.info("Phase 1: Loading raw data...")
        df = load_all_csv()
        logger.info(f"Loaded {len(df)} records from {df['city'].nunique()} cities")

        # Phase 2: Clean data
        logger.info("Phase 2: Cleaning data...")
        df = clean_data(df)
        logger.info("Data cleaning completed")

        # Phase 2.5: Validate data quality
        logger.info("Phase 2.5: Validating data quality...")
        report = validate_data(df)
        report_path = save_quality_report(report, "data_quality")
        logger.info(f"Data quality report saved to {report_path}")
        logger.info(f"  - Total rows: {report['total_rows']}")
        logger.info(f"  - Total columns: {report['total_columns']}")
        logger.info(f"  - Duplicates: {report['duplicates']}")

        # Save to CSV (processed data)
        project_root = Path(__file__).parent.parent.parent
        processed_dir = project_root / "data" / "processed"
        processed_dir.mkdir(parents=True, exist_ok=True)
        
        processed_path = processed_dir / "clean_dataset.csv"
        processed_path.unlink(missing_ok=True)
        df.to_csv(processed_path, index=False)
        logger.info(f"Saved cleaned dataset to {processed_path}")

        # Phase 3: Export to Parquet Data Lake
        logger.info("Phase 3: Exporting to Parquet Data Lake...")
        data_lake_path = project_root / "data_lake"
        saved_files = export_to_parquet_lake(df, data_lake_path)
        logger.info(f"Exported {len(saved_files)} parquet files to data lake")

        # Phase 4: Load to DuckDB Warehouse
        logger.info("Phase 4: Loading to DuckDB Warehouse...")
        warehouse_path = project_root / "warehouse" / "air_quality.duckdb"
        conn = create_warehouse(warehouse_path)
        load_data_to_warehouse(conn, df)
        logger.info(f"DuckDB warehouse created at {warehouse_path}")
        conn.close()

        # Analytics
        logger.info("Generating analytics...")
        analytics = create_analytics(df)
        
        if analytics is not None:
            analytics_dir = project_root / "data" / "analytics"
            analytics_dir.mkdir(parents=True, exist_ok=True)
            
            analytics_path = analytics_dir / "summary_metrics.csv"
            analytics_path.unlink(missing_ok=True)
            analytics.to_csv(analytics_path, index=False)
            logger.info(f"Saved analytics to {analytics_path}")

        logger.info("=" * 60)
        logger.info("Pipeline completed successfully!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    run()
