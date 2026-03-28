
"""
Database Connection Module - AWS Glue Catalog
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Any

from src.data_processing.glue_data_loader import GlueDataLoader

logger = logging.getLogger(__name__)


class GlueConnection:
    """AWS Glue Catalog connection"""

    def __init__(self, config: Dict = None):
        """
        Initialize Glue connection

        Args:
            config: Configuration dictionary with database name and region
        """
        self.config = config or {}
        self.database_name = self.config.get('database', 'dealers')
        self.region = self.config.get('region', 'us-east-1')
        self.data_loader = GlueDataLoader(self.database_name, self.region)

        logger.info(f"GlueConnection initialized: database={self.database_name}, region={self.region}")

    def connect(self) -> bool:
        """Establish connection"""
        try:
            # Test connection by listing tables
            tables = self.data_loader.list_tables()
            logger.info(f"Connected to Glue Catalog. Found {len(tables)} tables")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Glue Catalog: {str(e)}")
            return False

    def disconnect(self):
        """Close connection"""
        if self.data_loader:
            self.data_loader.clear_cache()
        logger.info("Glue connection closed")

    def execute_query(self, query: str, params: Any = None) -> pd.DataFrame:
        """Execute query using Glue/Athena"""
        # This is a simplified implementation - the actual queries are handled by GlueDataLoader methods
        logger.debug(f"Query received: {query[:100]}...")

        # Return empty DataFrame as actual queries are handled by specific methods
        return pd.DataFrame()

    def get_dealers(self) -> List[str]:
        """Get list of dealers"""
        return self.data_loader.get_dealers()

    def get_products(self) -> List[str]:
        """Get list of products"""
        return self.data_loader.get_products()

    def get_regions(self) -> List[str]:
        """Get list of regions"""
        return self.data_loader.get_regions()

    def get_kpi_metrics(self, filters: Dict = None) -> Dict:
        """Get KPI metrics"""
        return self.data_loader.get_kpi_metrics(filters)

    def get_transaction_data(self, filters: Dict = None, page: int = 1, page_size: int = 20) -> pd.DataFrame:
        """Get transaction lineage data"""
        return self.data_loader.get_transaction_data(filters, page, page_size)

    def get_dealer_health_scores(self, filters: Dict = None) -> pd.DataFrame:
        """Get dealer health scores"""
        return self.data_loader.get_dealer_health_scores(filters)

    def get_strategic_insights(self) -> pd.DataFrame:
        """Get strategic insights"""
        return self.data_loader.get_strategic_insights()

    def get_table_data(self, table_key: str, filters: Dict = None, limit: int = None) -> pd.DataFrame:
        """Get data from a specific table"""
        return self.data_loader.get_table_data(table_key, filters=filters, limit=limit)

    def clear_cache(self):
        """Clear the cache"""
        self.data_loader.clear_cache()

    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        return self.data_loader.get_cache_stats()


def get_db_connection():
    """Get database connection (Glue Catalog)"""
    from .config import Config

    config = Config()
    db_config = config.get('database', {})

    # Use Glue connection by default
    conn = GlueConnection({
        'database': db_config.get('glue_database', 'dealers'),
        'region': db_config.get('region', 'us-east-1')
    })
    conn.connect()
    return conn


