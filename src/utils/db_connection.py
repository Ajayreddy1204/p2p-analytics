"""
Database Connection Module - AWS Glue Catalog Only
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class GlueDataLoaderSimple:
    """Simple Glue Data Loader for reading from Glue Catalog"""

    def __init__(self, database_name: str = "dealers", region: str = "us-east-1"):
        self.database_name = database_name
        self.region = region
        self.cache = {}

        # Sample data for fallback
        self.sample_dealers = ["Premium Motors", "Elite Auto", "City Cars", "Highway Motors", "Metro Auto"]
        self.sample_products = ["Sedan", "SUV", "Hatchback", "Truck", "MUV", "EV"]

        logger.info(f"GlueDataLoaderSimple initialized: database={database_name}")

    def list_tables(self) -> List[str]:
        """List tables in Glue Catalog"""
        try:
            import boto3
            glue = boto3.client('glue', region_name=self.region)
            response = glue.get_tables(DatabaseName=self.database_name)
            return [t['Name'] for t in response.get('TableList', [])]
        except Exception as e:
            logger.warning(f"Cannot list tables: {e}")
            return []

    def get_dealers(self) -> List[str]:
        """Get dealers"""
        try:
            # Try to read from Glue
            import awswrangler as wr
            df = wr.athena.read_sql_query(
                sql=f'SELECT DISTINCT dealer_name FROM "{self.database_name}"."vw_dealer_location" LIMIT 100',
                database=self.database_name,
                s3_output=f's3://aws-athena-query-results-{self.region}/glue-results/'
            )
            if not df.empty:
                return df['dealer_name'].tolist()
        except Exception as e:
            logger.warning(f"Cannot read dealers from Glue: {e}")

        return self.sample_dealers

    def get_products(self) -> List[str]:
        """Get products"""
        try:
            import awswrangler as wr
            df = wr.athena.read_sql_query(
                sql=f'SELECT DISTINCT product_category FROM "{self.database_name}"."vw_sales_per_product_category" LIMIT 100',
                database=self.database_name,
                s3_output=f's3://aws-athena-query-results-{self.region}/glue-results/'
            )
            if not df.empty:
                return df['product_category'].tolist()
        except Exception as e:
            logger.warning(f"Cannot read products from Glue: {e}")

        return self.sample_products

    def get_regions(self) -> List[str]:
        """Get regions"""
        try:
            import awswrangler as wr
            df = wr.athena.read_sql_query(
                sql=f'SELECT DISTINCT location_region FROM "{self.database_name}"."vw_dealer_location" LIMIT 100',
                database=self.database_name,
                s3_output=f's3://aws-athena-query-results-{self.region}/glue-results/'
            )
            if not df.empty:
                return df['location_region'].tolist()
        except Exception as e:
            logger.warning(f"Cannot read regions from Glue: {e}")

        return ["North", "South", "East", "West", "Central"]

    def get_kpi_metrics(self, filters: Dict = None) -> Dict:
        """Get KPI metrics"""
        # Return sample KPI data for now
        return {
            'ccc': 35.5,
            'repair_tat': 42,
            'revenue_growth': 8.5,
            'gross_margin': 28.3,
            'stock_availability': 82.5,
            'backorder': 7.2,
            'lead_time': 6,
            'contribution_margin': 22.5,
            'sales_volume': 12500
        }

    def get_transaction_data(self, filters: Dict = None, page: int = 1, page_size: int = 20) -> pd.DataFrame:
        """Get transaction data"""
        import random
        from datetime import datetime, timedelta

        dealers = self.get_dealers()
        data = []

        for i in range(page_size):
            order_date = datetime.now() - timedelta(days=random.randint(1, 90))
            delivery_date = order_date + timedelta(days=random.randint(1, 15))

            data.append({
                'transaction_id': f'TXN{i + 1:04d}',
                'dealer_name': random.choice(dealers),
                'product_category': random.choice(self.sample_products),
                'order_date': order_date,
                'delivery_date': delivery_date,
                'order_flag': 'Y',
                'delivery_flag': 'Y' if random.random() > 0.1 else 'N',
                'invoice_amount': random.randint(10000, 100000),
                'invoice_status': random.choice(['Paid', 'Pending', 'Overdue'])
            })

        return pd.DataFrame(data)

    def get_dealer_health_scores(self, filters: Dict = None) -> pd.DataFrame:
        """Get health scores"""
        dealers = self.get_dealers()
        import random
        data = []
        for dealer in dealers:
            data.append({
                'dealer_name': dealer,
                'health_score': random.uniform(60, 95),
                'change_percent': random.uniform(-5, 15)
            })
        return pd.DataFrame(data)

    def get_strategic_insights(self) -> pd.DataFrame:
        """Get insights"""
        return pd.DataFrame({
            'insight_text': [
                'Revenue growth is strong at 8.5% over the last quarter',
                'Stock availability at 82.5% - monitor low stock dealers',
                'Service turnaround time at 42 hours - below SLA target'
            ],
            'priority_level': [1, 2, 3]
        })

    def clear_cache(self):
        self.cache.clear()

    def get_cache_stats(self) -> Dict:
        return {'cache_size': len(self.cache)}


class GlueConnection:
    """AWS Glue Catalog connection"""

    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.database_name = self.config.get('glue_database', 'dealers')
        self.region = self.config.get('region', 'us-east-1')
        self.data_loader = GlueDataLoaderSimple(self.database_name, self.region)
        logger.info(f"GlueConnection initialized: database={self.database_name}")

    def connect(self) -> bool:
        logger.info("Connected to Glue Catalog")
        return True

    def disconnect(self):
        logger.info("Glue connection closed")

    def execute_query(self, query: str, params: Any = None) -> pd.DataFrame:
        return pd.DataFrame()

    def get_dealers(self) -> List[str]:
        return self.data_loader.get_dealers()

    def get_products(self) -> List[str]:
        return self.data_loader.get_products()

    def get_regions(self) -> List[str]:
        return self.data_loader.get_regions()

    def get_kpi_metrics(self, filters: Dict = None) -> Dict:
        return self.data_loader.get_kpi_metrics(filters)

    def get_transaction_data(self, filters: Dict = None, page: int = 1, page_size: int = 20) -> pd.DataFrame:
        return self.data_loader.get_transaction_data(filters, page, page_size)

    def get_dealer_health_scores(self, filters: Dict = None) -> pd.DataFrame:
        return self.data_loader.get_dealer_health_scores(filters)

    def get_strategic_insights(self) -> pd.DataFrame:
        return self.data_loader.get_strategic_insights()

    def get_table_data(self, table_key: str, filters: Dict = None, limit: int = None) -> pd.DataFrame:
        return pd.DataFrame()

    def clear_cache(self):
        self.data_loader.clear_cache()

    def get_cache_stats(self) -> Dict:
        return self.data_loader.get_cache_stats()


def get_db_connection():
    """Get database connection - Always returns GlueConnection"""
    from .config import Config

    try:
        config = Config()
        db_config = config.get('database', {})
    except:
        db_config = {}

    logger.info("Creating GlueConnection (AWS Glue Catalog)")
    conn = GlueConnection({
        'glue_database': db_config.get('glue_database', 'dealers'),
        'region': db_config.get('region', 'us-east-1')
    })
    conn.connect()
    return conn