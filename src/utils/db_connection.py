import pandas as pd
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import random

logger = logging.getLogger(__name__)


class GlueDataLoader:
    """Simple Glue Data Loader with sample data"""

    def __init__(self, database_name: str = "dealers", region: str = "us-east-1"):
        self.database_name = database_name
        self.region = region
        self.cache = {}

        # Sample data for the dashboard
        self.sample_dealers = [
            "Premium Motors", "Elite Auto", "City Cars", "Highway Motors",
            "Metro Auto", "Coast Motors", "Central Auto", "Northside Cars",
            "Southside Motors", "East End Auto"
        ]

        self.sample_products = ["Sedan", "SUV", "Hatchback", "Truck", "MUV", "EV"]
        self.sample_regions = ["North", "South", "East", "West", "Central"]

        logger.info(f"GlueDataLoader initialized with sample data")

    def get_dealers(self) -> List[str]:
        """Get list of dealers"""
        return self.sample_dealers

    def get_products(self) -> List[str]:
        """Get list of products"""
        return self.sample_products

    def get_regions(self) -> List[str]:
        """Get list of regions"""
        return self.sample_regions

    def get_kpi_metrics(self, filters: Dict = None) -> Dict:
        """Get KPI metrics with realistic sample data"""
        return {
            'ccc': round(random.uniform(25, 55), 1),
            'repair_tat': random.randint(24, 72),
            'revenue_growth': round(random.uniform(-2, 18), 1),
            'gross_margin': round(random.uniform(18, 42), 1),
            'stock_availability': round(random.uniform(65, 95), 1),
            'backorder': round(random.uniform(2, 15), 1),
            'lead_time': random.randint(3, 10),
            'contribution_margin': round(random.uniform(15, 35), 1),
            'sales_volume': random.randint(5000, 25000)
        }

    def get_transaction_data(self, filters: Dict = None, page: int = 1, page_size: int = 20) -> pd.DataFrame:
        """Get sample transaction data"""
        dealers = self.get_dealers()
        data = []

        for i in range(page_size):
            order_date = datetime.now() - timedelta(days=random.randint(1, 90))
            delivery_date = order_date + timedelta(days=random.randint(1, 15))

            data.append({
                'transaction_id': f'TXN{i + 1:04d}',
                'dealer_name': random.choice(dealers),
                'product_category': random.choice(self.sample_products),
                'product_desc': f'Product {random.choice(["X", "Y", "Z"])} {random.randint(1, 10)}',
                'order_date': order_date.strftime('%Y-%m-%d'),
                'order_flag': 'Y',
                'delivery_date': delivery_date.strftime('%Y-%m-%d'),
                'delivery_flag': 'Y' if random.random() > 0.1 else 'N',
                'lead_time_days': (delivery_date - order_date).days,
                'invoice_date': (delivery_date + timedelta(days=random.randint(1, 7))).strftime('%Y-%m-%d'),
                'invoice_flag': 'Y',
                'invoice_amount': random.randint(10000, 100000),
                'invoice_status': random.choice(['Paid', 'Pending', 'Overdue']),
                'warranty_status': random.choice(['Active', 'Expired'])
            })

        return pd.DataFrame(data)

    def get_dealer_health_scores(self, filters: Dict = None) -> pd.DataFrame:
        """Get dealer health scores"""
        dealers = self.get_dealers()
        data = []

        for dealer in dealers:
            health_score = random.uniform(55, 95)
            data.append({
                'dealer_name': dealer,
                'health_score': round(health_score, 1),
                'change_percent': round(random.uniform(-8, 12), 1),
                'last_updated': datetime.now()
            })

        return pd.DataFrame(data)

    def get_strategic_insights(self) -> pd.DataFrame:
        """Get strategic insights"""
        insights = [
            "7 dealers flagged as AT-Risk based on declining margins",
            "Revenue growth is positive at 8.5% across the network",
            "Stock availability improved by 5% this quarter",
            "Service turnaround time reduced by 12% in North region",
            "Cash conversion cycle optimized by 8 days on average"
        ]

        return pd.DataFrame({
            'insight_text': insights,
            'priority_level': list(range(1, len(insights) + 1)),
            'created_at': [datetime.now()] * len(insights)
        })

    def get_revenue_trend(self, filters: Dict = None) -> pd.DataFrame:
        """Get revenue trend data"""
        dealers = self.get_dealers()[:3]
        dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='ME')

        data = []
        for dealer in dealers:
            base_revenue = random.randint(50000, 150000)
            for date in dates:
                trend = 1 + (date.month - 1) * 0.05
                revenue = base_revenue * trend * random.uniform(0.9, 1.1)
                data.append({
                    'dealer_name': dealer,
                    'period': date,
                    'total_revenue': round(revenue, 0)
                })

        return pd.DataFrame(data)

    def get_profit_margin_by_dealer(self, filters: Dict = None) -> pd.DataFrame:
        """Get profit margin by dealer"""
        dealers = self.get_dealers()
        data = []

        for dealer in dealers:
            data.append({
                'dealer_name': dealer,
                'gross_profit_margin_pct': round(random.uniform(15, 45), 1),
                'total_revenue': random.randint(200000, 2000000)
            })

        return pd.DataFrame(data).sort_values('gross_profit_margin_pct', ascending=False)

    def get_sales_by_product(self, filters: Dict = None) -> pd.DataFrame:
        """Get sales by product category"""
        data = []
        for product in self.sample_products:
            data.append({
                'product_category': product,
                'total_revenue': random.randint(50000, 500000),
                'total_quantity': random.randint(100, 5000)
            })

        return pd.DataFrame(data)

    def get_cash_conversion_cycle_trend(self, filters: Dict = None) -> pd.DataFrame:
        """Get CCC by dealer"""
        dealers = self.get_dealers()[:10]
        data = []

        for dealer in dealers:
            data.append({
                'dealer_name': dealer,
                'dso': round(random.uniform(25, 55), 1),
                'dio': round(random.uniform(20, 45), 1),
                'dpo': round(random.uniform(15, 35), 1),
                'ccc': round(random.uniform(30, 65), 1)
            })

        return pd.DataFrame(data).sort_values('ccc', ascending=False)

    def get_lead_time_distribution(self, filters: Dict = None) -> pd.DataFrame:
        """Get lead time by dealer"""
        dealers = self.get_dealers()[:10]
        data = []

        for dealer in dealers:
            data.append({
                'dealer_name': dealer,
                'avg_lead_time': round(random.uniform(3, 12), 1),
                'order_count': random.randint(50, 500)
            })

        return pd.DataFrame(data).sort_values('avg_lead_time', ascending=False)

    def get_journey_counts(self, filters: Dict = None) -> pd.DataFrame:
        """Get journey stage counts"""
        return pd.DataFrame([{
            'order_count': random.randint(150, 300),
            'delivery_count': random.randint(120, 280),
            'invoice_count': random.randint(100, 260),
            'paid_count': random.randint(80, 240),
            'warranty_count': random.randint(50, 200),
            'avg_lead_days': round(random.uniform(3, 8), 1)
        }])

    def clear_cache(self):
        self.cache.clear()

    def get_cache_stats(self) -> Dict:
        return {'cache_size': len(self.cache), 'mode': 'sample_data'}


class GlueConnection:
    """AWS Glue Catalog connection with sample data"""

    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.database_name = self.config.get('glue_database', 'dealers')
        self.region = self.config.get('region', 'us-east-1')
        self.data_loader = GlueDataLoader(self.database_name, self.region)
        logger.info(f"GlueConnection initialized with sample data")

    def connect(self) -> bool:
        logger.info("Connected to Glue Catalog (using sample data)")
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

    def get_revenue_trend(self, filters: Dict = None) -> pd.DataFrame:
        return self.data_loader.get_revenue_trend(filters)

    def get_profit_margin_by_dealer(self, filters: Dict = None) -> pd.DataFrame:
        return self.data_loader.get_profit_margin_by_dealer(filters)

    def get_sales_by_product(self, filters: Dict = None) -> pd.DataFrame:
        return self.data_loader.get_sales_by_product(filters)

    def get_cash_conversion_cycle_trend(self, filters: Dict = None) -> pd.DataFrame:
        return self.data_loader.get_cash_conversion_cycle_trend(filters)

    def get_lead_time_distribution(self, filters: Dict = None) -> pd.DataFrame:
        return self.data_loader.get_lead_time_distribution(filters)

    def get_journey_counts(self, filters: Dict = None) -> pd.DataFrame:
        return self.data_loader.get_journey_counts(filters)

    def clear_cache(self):
        self.data_loader.clear_cache()

    def get_cache_stats(self) -> Dict:
        return self.data_loader.get_cache_stats()


def get_db_connection():
    """Get database connection - Always returns GlueConnection with sample data"""
    logger.info("Creating GlueConnection with sample data")
    conn = GlueConnection({
        'glue_database': 'dealers',
        'region': 'us-east-1'
    })
    conn.connect()
    return conn

