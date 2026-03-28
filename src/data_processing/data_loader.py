"""
Data Loader Module
Handles database operations and caching for data retrieval
"""

import pandas as pd
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)


class DataLoader:
    """Load and cache data from database"""

    def __init__(self, db_connection, cache_ttl_seconds: int = 3600):
        """
        Initialize data loader with database connection and cache settings

        Args:
            db_connection: Database connection object
            cache_ttl_seconds: Time to live for cached data in seconds
        """
        self.db = db_connection
        self.cache_ttl = cache_ttl_seconds
        self.cache = {}
        self.logger = logging.getLogger(__name__)

    def _get_cache_key(self, query: str, params: Dict = None) -> str:
        """Generate cache key from query and parameters"""
        key_data = query
        if params:
            key_data += json.dumps(params, sort_keys=True)
        return hashlib.md5(key_data.encode()).hexdigest()

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached data is still valid"""
        if cache_key not in self.cache:
            return False
        return (datetime.now() - self.cache[cache_key]['timestamp']).seconds < self.cache_ttl

    def execute_query(self, query: str, params: Dict = None, use_cache: bool = True) -> pd.DataFrame:
        """
        Execute SQL query with optional caching

        Args:
            query: SQL query string
            params: Query parameters (for parameterized queries)
            use_cache: Whether to use cached results

        Returns:
            Pandas DataFrame with query results
        """
        try:
            cache_key = self._get_cache_key(query, params)

            # Return cached data if valid
            if use_cache and self._is_cache_valid(cache_key):
                self.logger.debug(f"Cache hit for query: {query[:50]}...")
                return self.cache[cache_key]['data'].copy()

            # Execute query
            if params:
                result = self.db.execute_query(query, params)
            else:
                result = self.db.execute_query(query)

            # Cache result
            if use_cache and result is not None and not result.empty:
                self.cache[cache_key] = {
                    'data': result.copy(),
                    'timestamp': datetime.now()
                }

            return result

        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            raise

    def fetch_dealers(self) -> List[str]:
        """Fetch list of all dealers"""
        query = """
        SELECT DISTINCT dealer_name
        FROM dealer_information_mart.vw_cash_conversion_cycle
        WHERE dealer_name IS NOT NULL
        ORDER BY dealer_name
        """

        result = self.execute_query(query)

        if result and not result.empty:
            return result['dealer_name'].tolist()

        return []

    def fetch_products(self) -> List[str]:
        """Fetch list of all product categories"""
        query = """
        SELECT DISTINCT product_category
        FROM dealer_information_mart.vw_sales_per_product_category
        WHERE product_category IS NOT NULL
        ORDER BY product_category
        """

        result = self.execute_query(query)

        if result and not result.empty:
            return result['product_category'].tolist()

        return []

    def fetch_regions(self) -> List[str]:
        """Fetch list of all regions"""
        query = """
        SELECT DISTINCT region
        FROM dealer_information_mart.vw_dealer_location
        WHERE region IS NOT NULL
        ORDER BY region
        """

        result = self.execute_query(query)

        if result and not result.empty:
            return result['region'].tolist()

        return []

    def fetch_dealer_health_scores(self, filters: Dict = None) -> pd.DataFrame:
        """
        Fetch dealer health scores with optional filters

        Args:
            filters: Dictionary with dealer, date_range filters

        Returns:
            DataFrame with dealer health scores
        """
        query = """
        WITH dealer_scores AS (
            SELECT 
                d.dealer_name,
                AVG(g.gross_profit_margin_pct) as gross_margin,
                AVG(r.revenue_growth_mom_percent) as revenue_growth,
                AVG(c.ccc) as cash_cycle,
                AVG(t.avg_turnaround_hours) as repair_tat,
                AVG(s.stock_availability_pct) as stock_avail
            FROM dealer_information_mart.vw_dealer_location d
            LEFT JOIN dealer_information_mart.vw_gross_profit_margin g ON d.dealer_name = g.dealer_name
            LEFT JOIN dealer_information_mart.vw_dealer_revenue_growth r ON d.dealer_name = r.dealer_name
            LEFT JOIN dealer_information_mart.vw_cash_conversion_cycle c ON d.dealer_name = c.dealer_name
            LEFT JOIN dealer_information_mart.vw_average_repair_turnaround_time t ON d.dealer_name = t.dealer_name
            LEFT JOIN dealer_information_mart.vw_stock_availability_dealer s ON d.dealer_name = s.dealer_name
            WHERE 1=1
        """

        if filters:
            if filters.get('dealer') and filters['dealer'] != 'All Dealers':
                query += f" AND d.dealer_name = '{filters['dealer']}'"

            if filters.get('region') and filters['region'] != 'All Regions':
                query += f" AND d.region = '{filters['region']}'"

            if filters.get('from_date') and filters.get('to_date'):
                query += f" AND g.period_month >= DATE_TRUNC('month', '{filters['from_date']}')"
                query += f" AND g.period_month <= DATE_TRUNC('month', '{filters['to_date']}')"

        query += """
            GROUP BY d.dealer_name
        )
        SELECT 
            dealer_name,
            COALESCE(gross_margin, 0) as gross_margin,
            COALESCE(revenue_growth, 0) as revenue_growth,
            COALESCE(cash_cycle, 0) as cash_cycle,
            COALESCE(repair_tat, 0) as repair_tat,
            COALESCE(stock_avail, 0) as stock_avail,
            CASE 
                WHEN gross_margin > 30 THEN 100
                WHEN gross_margin > 20 THEN 75
                WHEN gross_margin > 10 THEN 50
                ELSE 25
            END as margin_score,
            CASE 
                WHEN revenue_growth > 10 THEN 100
                WHEN revenue_growth > 5 THEN 75
                WHEN revenue_growth > 0 THEN 50
                ELSE 25
            END as growth_score,
            CASE 
                WHEN cash_cycle < 30 THEN 100
                WHEN cash_cycle < 45 THEN 75
                WHEN cash_cycle < 60 THEN 50
                ELSE 25
            END as cash_score,
            CASE 
                WHEN repair_tat < 24 THEN 100
                WHEN repair_tat < 36 THEN 75
                WHEN repair_tat < 48 THEN 50
                ELSE 25
            END as service_score,
            CASE 
                WHEN stock_avail > 85 THEN 100
                WHEN stock_avail > 70 THEN 75
                WHEN stock_avail > 50 THEN 50
                ELSE 25
            END as inventory_score
        FROM dealer_scores
        ORDER BY dealer_name
        """

        result = self.execute_query(query)

        if result and not result.empty:
            # Calculate overall health score
            result['health_score'] = (
                    result['margin_score'] * 0.25 +
                    result['growth_score'] * 0.20 +
                    result['cash_score'] * 0.15 +
                    result['service_score'] * 0.20 +
                    result['inventory_score'] * 0.20
            )

            return result

        return pd.DataFrame()

    def fetch_transaction_lineage(self, filters: Dict = None, page: int = 1, page_size: int = 20) -> pd.DataFrame:
        """
        Fetch paginated transaction lineage data

        Args:
            filters: Dictionary with filters (dealer, date_range, etc.)
            page: Page number (1-indexed)
            page_size: Number of rows per page

        Returns:
            DataFrame with transaction lineage data
        """
        offset = (page - 1) * page_size

        where_clause = "WHERE 1=1"

        if filters:
            if filters.get('dealer') and filters['dealer'] != 'All Dealers':
                where_clause += f" AND dealer_name = '{filters['dealer']}'"

            if filters.get('transaction_id') and filters['transaction_id'] != 'All':
                where_clause += f" AND transaction_id = '{filters['transaction_id']}'"

            if filters.get('product') and filters['product'] != 'Product':
                where_clause += f" AND product_category ILIKE '%{filters['product']}%'"

            if filters.get('paid') and filters['paid'] != 'All':
                where_clause += f" AND paid_flag = '{filters['paid']}'"

            if filters.get('invoice_status') and filters['invoice_status'] != 'All':
                where_clause += f" AND invoice_status = '{filters['invoice_status']}'"

            if filters.get('warranty_status') and filters['warranty_status'] != 'All':
                where_clause += f" AND warranty_status = '{filters['warranty_status']}'"

            if filters.get('from_date') and filters.get('to_date'):
                where_clause += f" AND order_date >= '{filters['from_date']}'"
                where_clause += f" AND order_date <= '{filters['to_date']}'"

        query = f"""
        SELECT 
            transaction_id,
            dealer_name,
            product_category,
            product_desc,
            order_date,
            order_flag as order_done,
            delivery_date,
            delivery_flag as delivery_done,
            lead_time_days,
            invoice_date,
            payment_date,
            invoice_flag as invoice_done,
            invoice_amount,
            invoice_status,
            warranty_end_date,
            warranty_status
        FROM dealer_information_mart.vw_transaction_lineage
        {where_clause}
        ORDER BY order_date DESC
        LIMIT {page_size} OFFSET {offset}
        """

        result = self.execute_query(query)

        if result and not result.empty:
            return result

        return pd.DataFrame()

    def fetch_journey_counts(self, filters: Dict = None) -> pd.DataFrame:
        """
        Fetch journey stage counts for the timeline

        Args:
            filters: Dictionary with filters (dealer, date_range, etc.)

        Returns:
            DataFrame with journey counts
        """
        where_clause = "WHERE 1=1"

        if filters:
            if filters.get('dealer') and filters['dealer'] != 'All Dealers':
                where_clause += f" AND dealer_name = '{filters['dealer']}'"

            if filters.get('transaction_id') and filters['transaction_id'] != 'All':
                where_clause += f" AND transaction_id = '{filters['transaction_id']}'"

            if filters.get('from_date') and filters.get('to_date'):
                where_clause += f" AND order_date >= '{filters['from_date']}'"
                where_clause += f" AND order_date <= '{filters['to_date']}'"

        query = f"""
        SELECT 
            SUM(CASE WHEN order_flag = 'Y' THEN 1 ELSE 0 END) as order_count,
            SUM(CASE WHEN delivery_flag = 'Y' THEN 1 ELSE 0 END) as delivery_count,
            SUM(CASE WHEN invoice_flag = 'Y' THEN 1 ELSE 0 END) as invoice_count,
            SUM(CASE WHEN paid_flag = 'Y' THEN 1 ELSE 0 END) as paid_count,
            SUM(CASE WHEN warranty_flag = 'Y' THEN 1 ELSE 0 END) as warranty_count,
            SUM(CASE WHEN warranty_status = 'ACTIVE' THEN 1 ELSE 0 END) as active_warranty_count,
            SUM(CASE WHEN warranty_status = 'EXPIRED' THEN 1 ELSE 0 END) as expired_warranty_count,
            ROUND(AVG(
                CASE WHEN delivery_date IS NOT NULL AND order_date IS NOT NULL
                     THEN DATEDIFF(day, order_date, delivery_date)
                     ELSE lead_time_days
                END
            ), 1) as avg_lead_days
        FROM dealer_information_mart.vw_transaction_lineage
        {where_clause}
        """

        result = self.execute_query(query)

        if result and not result.empty:
            return result

        return pd.DataFrame([{
            'order_count': 0,
            'delivery_count': 0,
            'invoice_count': 0,
            'paid_count': 0,
            'warranty_count': 0,
            'active_warranty_count': 0,
            'expired_warranty_count': 0,
            'avg_lead_days': 0
        }])

    def fetch_sales_by_product_category(self, filters: Dict = None) -> pd.DataFrame:
        """
        Fetch sales aggregated by product category

        Args:
            filters: Dictionary with dealer, date_range filters

        Returns:
            DataFrame with sales by product category
        """
        where_clause = "WHERE 1=1"

        if filters:
            if filters.get('dealer') and filters['dealer'] != 'All Dealers':
                where_clause += f" AND dealer_name = '{filters['dealer']}'"

            if filters.get('from_date') and filters.get('to_date'):
                where_clause += f" AND period_start_date >= '{filters['from_date']}'"
                where_clause += f" AND period_start_date <= '{filters['to_date']}'"

        query = f"""
        SELECT 
            product_category,
            SUM(total_revenue) as total_revenue,
            SUM(total_quantity) as total_quantity
        FROM dealer_information_mart.vw_sales_per_product_category
        {where_clause}
        GROUP BY product_category
        ORDER BY total_revenue DESC
        LIMIT 20
        """

        result = self.execute_query(query)

        if result and not result.empty:
            return result

        return pd.DataFrame()

    def fetch_revenue_trend(self, filters: Dict = None) -> pd.DataFrame:
        """
        Fetch revenue trend over time

        Args:
            filters: Dictionary with dealer, date_range filters

        Returns:
            DataFrame with revenue trend data
        """
        where_clause = "WHERE 1=1"

        if filters:
            if filters.get('dealer') and filters['dealer'] != 'All Dealers':
                where_clause += f" AND dealer_name = '{filters['dealer']}'"

            if filters.get('from_date') and filters.get('to_date'):
                where_clause += f" AND period_month >= DATE_TRUNC('month', '{filters['from_date']}')"
                where_clause += f" AND period_month <= DATE_TRUNC('month', '{filters['to_date']}')"

        query = f"""
        SELECT 
            dealer_name,
            period_month as period,
            SUM(revenue) as total_revenue
        FROM dealer_information_mart.vw_dealer_revenue_growth
        {where_clause}
        GROUP BY dealer_name, period_month
        ORDER BY period ASC
        """

        result = self.execute_query(query)

        if result and not result.empty:
            return result

        return pd.DataFrame()

    def fetch_profit_margin_by_dealer(self, filters: Dict = None) -> pd.DataFrame:
        """
        Fetch profit margin by dealer

        Args:
            filters: Dictionary with date_range filters

        Returns:
            DataFrame with profit margins by dealer
        """
        where_clause = "WHERE 1=1"

        if filters:
            if filters.get('from_date') and filters.get('to_date'):
                where_clause += f" AND period_month >= DATE_TRUNC('month', '{filters['from_date']}')"
                where_clause += f" AND period_month <= DATE_TRUNC('month', '{filters['to_date']}')"

        query = f"""
        SELECT 
            dealer_name,
            AVG(gross_profit_margin_pct) as gross_profit_margin_pct,
            SUM(total_revenue) as total_revenue
        FROM dealer_information_mart.vw_gross_profit_margin
        {where_clause}
        WHERE dealer_name IS NOT NULL
        GROUP BY dealer_name
        ORDER BY gross_profit_margin_pct DESC
        LIMIT 15
        """

        result = self.execute_query(query)

        if result and not result.empty:
            return result

        return pd.DataFrame()

    def fetch_cash_conversion_cycle_trend(self, filters: Dict = None) -> pd.DataFrame:
        """
        Fetch cash conversion cycle trend

        Args:
            filters: Dictionary with date_range filters

        Returns:
            DataFrame with CCC data
        """
        where_clause = "WHERE 1=1"

        if filters:
            if filters.get('from_date') and filters.get('to_date'):
                where_clause += f" AND period_month >= DATE_TRUNC('month', '{filters['from_date']}')"
                where_clause += f" AND period_month <= DATE_TRUNC('month', '{filters['to_date']}')"

        query = f"""
        SELECT 
            dealer_name,
            AVG(dso) as dso,
            AVG(dio) as dio,
            AVG(dpo) as dpo,
            AVG(ccc) as ccc
        FROM dealer_information_mart.vw_cash_conversion_cycle
        {where_clause}
        WHERE dealer_name IS NOT NULL
        GROUP BY dealer_name
        ORDER BY ccc DESC
        LIMIT 10
        """

        result = self.execute_query(query)

        if result and not result.empty:
            return result

        return pd.DataFrame()

    def fetch_order_lead_time_distribution(self, filters: Dict = None) -> pd.DataFrame:
        """
        Fetch order lead time distribution by dealer

        Args:
            filters: Dictionary with date_range filters

        Returns:
            DataFrame with lead time data
        """
        where_clause = "WHERE 1=1"

        if filters:
            if filters.get('from_date') and filters.get('to_date'):
                where_clause += f" AND period_start_date >= '{filters['from_date']}'"
                where_clause += f" AND period_start_date <= '{filters['to_date']}'"

        query = f"""
        SELECT 
            dealer_name,
            AVG(avg_order_lead_time_days) as avg_lead_time,
            COUNT(*) as order_count
        FROM dealer_information_mart.vw_order_lead_time
        {where_clause}
        GROUP BY dealer_name
        ORDER BY avg_lead_time DESC
        LIMIT 10
        """

        result = self.execute_query(query)

        if result and not result.empty:
            return result

        return pd.DataFrame()

    def clear_cache(self):
        """Clear all cached data"""
        self.cache.clear()
        self.logger.info("Cache cleared")

    def get_cache_stats(self) -> Dict:
        """
        Get cache statistics

        Returns:
            Dictionary with cache statistics
        """
        valid_entries = 0
        total_entries = len(self.cache)
        oldest_entry = None
        newest_entry = None

        for key, entry in self.cache.items():
            if self._is_cache_valid(key):
                valid_entries += 1

                timestamp = entry['timestamp']
                if oldest_entry is None or timestamp < oldest_entry:
                    oldest_entry = timestamp
                if newest_entry is None or timestamp > newest_entry:
                    newest_entry = timestamp

        return {
            'total_entries': total_entries,
            'valid_entries': valid_entries,
            'invalid_entries': total_entries - valid_entries,
            'oldest_timestamp': oldest_entry,
            'newest_timestamp': newest_entry,
            'cache_ttl_seconds': self.cache_ttl
        }
    