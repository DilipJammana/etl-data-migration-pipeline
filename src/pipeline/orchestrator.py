"""
ETL Pipeline Orchestrator
Main entry point for the ETL data migration pipeline
"""

import sys
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
import yaml
from loguru import logger

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.extract.csv_extractor import CSVExtractor
from src.extract.db_extractor import DatabaseExtractor
from src.transform.cleaner import DataCleaner
from src.transform.validator import DataValidator
from src.load.db_loader import DatabaseLoader
from src.utils.metrics import PipelineMetrics
from src.utils.error_handler import ErrorHandler


class ETLOrchestrator:
    """
    Main orchestrator for ETL pipeline execution
    Coordinates extract, transform, and load operations
    """
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize orchestrator with configuration"""
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.metrics = PipelineMetrics()
        self.error_handler = ErrorHandler()
        
        logger.info("ETL Orchestrator initialized")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _setup_logging(self):
        """Configure logging with rotation and format"""
        log_config = self.config.get('monitoring', {})
        log_level = log_config.get('log_level', 'INFO')
        log_path = log_config.get('log_path', 'logs/etl_pipeline.log')
        
        # Remove default logger
        logger.remove()
        
        # Add console logger
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
            level=log_level
        )
        
        # Add file logger with rotation
        logger.add(
            log_path,
            rotation="100 MB",
            retention="30 days",
            compression="zip",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level=log_level
        )
    
    def run_full_pipeline(self, table: Optional[str] = None):
        """
        Execute full ETL pipeline
        
        Args:
            table: Specific table to process, or None for all tables
        """
        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting full ETL pipeline - Run ID: {run_id}")
        
        self.metrics.start_pipeline(run_id)
        
        try:
            # Phase 1: Extract
            logger.info("=" * 60)
            logger.info("PHASE 1: EXTRACT")
            logger.info("=" * 60)
            extracted_data = self._extract_phase(table)
            
            if not extracted_data:
                logger.warning("No data extracted. Aborting pipeline.")
                return
            
            # Phase 2: Transform
            logger.info("=" * 60)
            logger.info("PHASE 2: TRANSFORM")
            logger.info("=" * 60)
            transformed_data = self._transform_phase(extracted_data)
            
            # Phase 3: Load
            logger.info("=" * 60)
            logger.info("PHASE 3: LOAD")
            logger.info("=" * 60)
            self._load_phase(transformed_data)
            
            # Pipeline completed successfully
            self.metrics.end_pipeline(run_id, status="SUCCESS")
            logger.success(f"ETL Pipeline completed successfully - Run ID: {run_id}")
            
            # Print summary
            self._print_summary(run_id)
            
        except Exception as e:
            self.metrics.end_pipeline(run_id, status="FAILED")
            logger.error(f"Pipeline failed: {str(e)}")
            self.error_handler.handle_error(e, context={"run_id": run_id})
            raise
    
    def run_incremental_pipeline(self, table: str):
        """
        Execute incremental ETL pipeline (only changed data)
        
        Args:
            table: Table name to process incrementally
        """
        run_id = f"incremental_{table}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting incremental ETL pipeline for {table} - Run ID: {run_id}")
        
        self.metrics.start_pipeline(run_id)
        
        try:
            # Extract only changed records
            logger.info(f"Extracting incremental data for {table}")
            extracted_data = self._extract_incremental(table)
            
            if extracted_data.empty:
                logger.info(f"No new/changed records for {table}")
                self.metrics.end_pipeline(run_id, status="SUCCESS_NO_DATA")
                return
            
            # Transform and load
            transformed_data = self._transform_phase({table: extracted_data})
            self._load_phase(transformed_data, mode="incremental")
            
            self.metrics.end_pipeline(run_id, status="SUCCESS")
            logger.success(f"Incremental pipeline completed - Run ID: {run_id}")
            
        except Exception as e:
            self.metrics.end_pipeline(run_id, status="FAILED")
            logger.error(f"Incremental pipeline failed: {str(e)}")
            self.error_handler.handle_error(e, context={"run_id": run_id, "table": table})
            raise
    
    def _extract_phase(self, table: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract data from all configured sources
        
        Args:
            table: Optional specific table to extract
            
        Returns:
            Dictionary of extracted dataframes by source
        """
        extracted_data = {}
        
        # Extract from CSV files
        csv_extractor = CSVExtractor(self.config['source']['csv_path'])
        csv_tables = [table] if table else ['orders', 'customers']
        
        for csv_table in csv_tables:
            logger.info(f"Extracting from CSV: {csv_table}")
            try:
                df = csv_extractor.extract(csv_table)
                extracted_data[csv_table] = df
                logger.info(f"Extracted {len(df)} records from {csv_table}")
                self.metrics.record_extraction(csv_table, len(df))
            except Exception as e:
                logger.error(f"Failed to extract {csv_table}: {str(e)}")
                self.error_handler.handle_error(e, context={"table": csv_table})
        
        # Extract from SQLite database
        if table is None or table == 'products':
            db_extractor = DatabaseExtractor(
                self.config['source']['sqlite_db'],
                db_type='sqlite'
            )
            
            logger.info("Extracting from SQLite: products")
            try:
                df = db_extractor.extract_table('products')
                extracted_data['products'] = df
                logger.info(f"Extracted {len(df)} records from products")
                self.metrics.record_extraction('products', len(df))
            except Exception as e:
                logger.error(f"Failed to extract products: {str(e)}")
                self.error_handler.handle_error(e, context={"table": "products"})
        
        return extracted_data
    
    def _extract_incremental(self, table: str):
        """Extract only changed records since last run"""
        # TODO: Implement incremental extraction logic
        # This would check last_modified timestamps or use CDC
        logger.info(f"Incremental extraction for {table} (CDC/timestamp-based)")
        pass
    
    def _transform_phase(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform extracted data
        
        Args:
            extracted_data: Dictionary of extracted dataframes
            
        Returns:
            Dictionary of transformed dataframes
        """
        transformed_data = {}
        
        cleaner = DataCleaner()
        validator = DataValidator(self.config.get('quality', {}))
        
        for table_name, df in extracted_data.items():
            logger.info(f"Transforming: {table_name}")
            
            try:
                # Step 1: Data Cleansing
                logger.info(f"  - Cleansing {table_name}")
                df_clean = cleaner.clean(df, table_name)
                
                # Step 2: Data Validation
                logger.info(f"  - Validating {table_name}")
                validation_result = validator.validate(df_clean, table_name)
                
                if not validation_result['is_valid']:
                    logger.warning(f"Validation warnings for {table_name}:")
                    for warning in validation_result['warnings']:
                        logger.warning(f"    - {warning}")
                
                # Step 3: Business Logic Transformations
                logger.info(f"  - Applying business logic to {table_name}")
                df_transformed = self._apply_business_logic(df_clean, table_name)
                
                transformed_data[table_name] = df_transformed
                logger.success(f"Transformed {len(df_transformed)} records for {table_name}")
                
                # Record metrics
                self.metrics.record_transformation(
                    table_name,
                    input_count=len(df),
                    output_count=len(df_transformed),
                    quality_score=validation_result.get('quality_score', 100)
                )
                
            except Exception as e:
                logger.error(f"Transformation failed for {table_name}: {str(e)}")
                self.error_handler.handle_error(e, context={"table": table_name, "phase": "transform"})
                raise
        
        return transformed_data
    
    def _apply_business_logic(self, df, table_name: str):
        """Apply table-specific business logic transformations"""
        
        if table_name == 'orders':
            # Calculate order totals, apply discounts, etc.
            if 'quantity' in df.columns and 'unit_price' in df.columns:
                df['total_amount'] = df['quantity'] * df['unit_price']
            
            # Add derived fields
            df['order_year'] = df['order_date'].dt.year
            df['order_month'] = df['order_date'].dt.month
            
        elif table_name == 'customers':
            # Standardize customer data
            if 'email' in df.columns:
                df['email'] = df['email'].str.lower().str.strip()
            
            # Create customer segments
            # (Would typically join with orders for RFM analysis)
            
        elif table_name == 'products':
            # Categorize products, calculate margins, etc.
            if 'price' in df.columns and 'cost' in df.columns:
                df['margin_pct'] = ((df['price'] - df['cost']) / df['price'] * 100).round(2)
        
        return df
    
    def _load_phase(self, transformed_data: Dict[str, Any], mode: str = "full"):
        """
        Load transformed data to target warehouse
        
        Args:
            transformed_data: Dictionary of transformed dataframes
            mode: 'full' or 'incremental'
        """
        loader = DatabaseLoader(self.config['target'])
        
        for table_name, df in transformed_data.items():
            logger.info(f"Loading: {table_name} ({mode} mode)")
            
            try:
                if mode == "full":
                    # Truncate and reload
                    records_loaded = loader.load_full(df, table_name)
                else:
                    # Upsert (insert/update)
                    records_loaded = loader.load_incremental(df, table_name)
                
                logger.success(f"Loaded {records_loaded} records to {table_name}")
                self.metrics.record_load(table_name, records_loaded)
                
            except Exception as e:
                logger.error(f"Load failed for {table_name}: {str(e)}")
                self.error_handler.handle_error(e, context={"table": table_name, "phase": "load"})
                raise
    
    def _print_summary(self, run_id: str):
        """Print pipeline execution summary"""
        summary = self.metrics.get_summary(run_id)
        
        logger.info("=" * 60)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Run ID: {run_id}")
        logger.info(f"Status: {summary['status']}")
        logger.info(f"Duration: {summary['duration_seconds']:.2f} seconds")
        logger.info(f"Records Extracted: {summary['total_extracted']}")
        logger.info(f"Records Loaded: {summary['total_loaded']}")
        logger.info(f"Quality Score: {summary['avg_quality_score']:.2f}%")
        logger.info("=" * 60)


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(description='ETL Data Migration Pipeline')
    parser.add_argument(
        '--mode',
        choices=['full', 'incremental'],
        default='full',
        help='Pipeline execution mode'
    )
    parser.add_argument(
        '--table',
        type=str,
        help='Specific table to process (optional)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Path to configuration file'
    )
    
    args = parser.parse_args()
    
    try:
        orchestrator = ETLOrchestrator(config_path=args.config)
        
        if args.mode == 'full':
            orchestrator.run_full_pipeline(table=args.table)
        else:
            if not args.table:
                logger.error("--table is required for incremental mode")
                sys.exit(1)
            orchestrator.run_incremental_pipeline(table=args.table)
            
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
