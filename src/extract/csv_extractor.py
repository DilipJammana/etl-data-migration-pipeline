"""
CSV Data Extractor
Handles extraction from CSV files with validation and error handling
"""

import pandas as pd
from pathlib import Path
from typing import Optional, List
from loguru import logger


class CSVExtractor:
    """Extract data from CSV files with robust error handling"""
    
    def __init__(self, source_path: str):
        """
        Initialize CSV extractor
        
        Args:
            source_path: Path to directory containing CSV files
        """
        self.source_path = Path(source_path)
        if not self.source_path.exists():
            raise FileNotFoundError(f"Source path does not exist: {source_path}")
        
        logger.info(f"CSV Extractor initialized with path: {source_path}")
    
    def extract(self, table_name: str, encoding: str = 'utf-8') -> pd.DataFrame:
        """
        Extract data from CSV file
        
        Args:
            table_name: Name of the table (without .csv extension)
            encoding: File encoding (default: utf-8)
            
        Returns:
            DataFrame containing extracted data
        """
        file_path = self.source_path / f"{table_name}.csv"
        
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        logger.info(f"Extracting from: {file_path}")
        
        try:
            # Read CSV with pandas
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                parse_dates=self._get_date_columns(table_name),
                na_values=['', 'NULL', 'null', 'N/A', 'NA']
            )
            
            # Add extraction metadata
            df['_extracted_at'] = pd.Timestamp.now()
            df['_source_file'] = file_path.name
            
            logger.info(f"Successfully extracted {len(df)} rows from {file_path.name}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract from {file_path}: {str(e)}")
            raise
    
    def extract_with_schema(self, table_name: str, schema: dict) -> pd.DataFrame:
        """
        Extract CSV with explicit schema validation
        
        Args:
            table_name: Name of the table
            schema: Dictionary mapping column names to data types
            
        Returns:
            DataFrame with enforced schema
        """
        df = self.extract(table_name)
        
        # Validate columns
        missing_cols = set(schema.keys()) - set(df.columns)
        if missing_cols:
            logger.warning(f"Missing expected columns: {missing_cols}")
        
        # Apply data types
        for col, dtype in schema.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except Exception as e:
                    logger.warning(f"Could not convert {col} to {dtype}: {str(e)}")
        
        return df
    
    def list_available_files(self) -> List[str]:
        """List all CSV files in source directory"""
        csv_files = list(self.source_path.glob('*.csv'))
        return [f.stem for f in csv_files]
    
    def _get_date_columns(self, table_name: str) -> List[str]:
        """Return list of date columns for specific table"""
        date_columns_map = {
            'orders': ['order_date', 'ship_date'],
            'customers': ['registration_date', 'last_purchase_date'],
            'products': ['created_at', 'updated_at']
        }
        return date_columns_map.get(table_name, [])
