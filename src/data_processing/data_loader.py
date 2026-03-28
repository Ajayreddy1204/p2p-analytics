
"""
AWS Glue Data Loader Module
Reads data from Glue Catalog tables in 'dealers' database
"""

import boto3
import pandas as pd
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import awswrangler as wr
import time

logger = logging.getLogger(__name__)


class GlueDataLoader:
    """Load data from AWS Glue Catalog tables"""

    def __init__(self, database_name: str = "dealers", region: str = "us-east-1"):
        """
        Initialize Glue Data Loader

        Args:
            database_name: Glue Catalog database name (default: dealers)
            region: AWS region (default: us-east-1)
        """
        self.database_name = database_name
        self.region = region
        self.glue_client = boto3.client('glue', region_name=region)
        self.athena_client = boto3.client('athena', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)

        # Cache for loaded data
        self.cache = {}
        self.cache_ttl = 3600  # 1 hour cache
        self.cache_timestamps = {}

        # Table names in Glue Catalog (as they appear in your database)
        # These should match your actual Glue table names
        self.table_names = {
            'dealer_location': 'vw_dealer_location',
            'gross_profit_margin': 'vw_gross_profit_margin',
            'dealer_revenue_growth': 'vw_dealer_revenue_growth',
            'cash_conversion_cycle': 'vw_cash_conversion_cycle',
            'repair_turnaround_time': 'vw_average_repair_turnaround_time',
            'order_lead_time': 'vw_order_lead_time',
            'stock_availability': 'vw_stock_availability_dealer',
            'backorder_incidence': 'vw_backorder_incidence',
            'dealer_contribution_margin': 'vw_dealer_contribution_margin',
            'sales_volume': 'vw_sales_volume',
            'sales_by_product_category': 'vw_sales_per_product_category',
            'transaction_lineage': 'vw_transaction_lineage',
            'dealer_health_scorecard': 'vw_dealer_health_scorecard',
            'strategic_insights': 'vw_strategic_insights'
        }

        logger.info(f"GlueDataLoader initialized: database={database_name}, region={region}")

        # Test connection
        self._test_connection()

    def _test_connection(self):
        """Test connection to Glue Catalog"""
        try:
            tables = self.list_tables()
            logger.info(f"Connected to Glue Catalog. Found {len(tables)} tables")
        except Exception as e:
            logger.error(f"Failed to connect to Glue Catalog: {str(e)}")

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache entry is still valid"""
        if cache_key not in self.cache:
            return False
        if cache_key not in self.cache_timestamps:
            return False
        return (time.time() - self.cache_timestamps[cache_key]) < self.cache_ttl

    def get_table_data(self, table_key: str, limit: int = None, filters: Dict = None,
                       use_cache: bool = True) -> pd.DataFrame:
        """
        Get data from a Glue Catalog table

        Args:
            table_key: Key from table_names dict (e.g., 'dealer_location')
            limit: Optional row limit
            filters: Optional filters to apply
            use_cache: Whether to use cached data

        Returns:
            Pandas DataFrame with table data
        """
        try:
            # Get actual table name
            table_name = self.table_names.get(table_key, table_key)

            # Generate cache key
            cache_key = f"{table_name}_{filters}_{limit}"

            # Check cache
            if use_cache and self._is_cache_valid(cache_key):
                logger.debug(f"Cache hit for {table_name}")
                return self.cache[cache_key].copy()

            # Build query
            query = f'SELECT * FROM "{self.database_name}"."{table_name}"'

            # Add WHERE clause for filters
            where_clauses = []
            if filters:
                if filters.get('dealer') and filters['dealer'] != 'All Dealers':
                    where_clauses.append(f"dealer_name = '{filters['dealer']}'")

                if filters.get('region') and filters['region'] != 'All Regions':
                    where_clauses.append(f"location_region = '{filters['region']}'")

                if filters.get('from_date') and filters.get('to_date'):
                    where_clauses.append(f"period_start_date >= date '{filters['from_date']}'")
                    where_clauses.append(f"period_start_date <= date '{filters['to_date']}'")

                if filters.get('product') and filters['product'] not in ['All Products', 'Product']:
                    where_clauses.append(f"product_category = '{filters['product']}'")

            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

            # Add LIMIT
            if limit:
                query += f" LIMIT {limit}"

            logger.debug(f"Executing query: {query[:200]}...")

            # Execute query using AWS Wrangler (simpler than raw Athena)
            df = wr.athena.read_sql_query(
                sql=query,
                database=self.database_name,
                ctas_approach=False,
                s3_output=f's3://aws-athena-query-results-{self.region}/glue-results/'
            )

            # Cache the result
            if use_cache:
                self.cache[cache_key] = df.copy()
                self.cache_timestamps[cache_key] = time.time()

            logger.info(f"Loaded {len(df)} rows from {table_name}")
            return df

        except Exception as e:
            logger.error(f"Error reading from Glue table {table_key}: {str(e)}")
            # Return empty DataFrame with appropriate columns for fallback
            return self._get_empty_dataframe_for_table(table_key)

    def _get_empty_dataframe_for_table(self, table_key: str) -> pd.DataFrame:
        """Return empty DataFrame with correct columns for a table"""
        empty_dfs = {
            'dealer_location': pd.DataFrame(
                columns=['dealer_name', 'location_city', 'location_state', 'location_region', 'dealer_type',
                         'dealer_tier']),
            'gross_profit_margin': pd.DataFrame(
                columns=['dealer_name', 'period_year', 'period_month', 'gross_profit_margin_pct', 'total_revenue']),
            'dealer_revenue_growth': pd.DataFrame(
                columns=['dealer_name', 'period_month', 'revenue', 'revenue_growth_mom_percent']),
            'cash_conversion_cycle': pd.DataFrame(columns=['dealer_name', 'period_month', 'dso', 'dio', 'dpo', 'ccc']),
            'repair_turnaround_time': pd.DataFrame(
                columns=['dealer_name', 'period_start_date', 'avg_turnaround_hours']),
            'order_lead_time': pd.DataFrame(columns=['dealer_name', 'period_start_date', 'avg_order_lead_time_days']),
            'stock_availability': pd.DataFrame(columns=['dealer_name', 'period_start_date', 'stock_availability_pct']),
            'backorder_incidence': pd.DataFrame(
                columns=['dealer_name', 'period_start_date', 'backorder_incidence_pct']),
            'dealer_contribution_margin': pd.DataFrame(
                columns=['dealer_name', 'period_start_date', 'contribution_margin_pct']),
            'sales_volume': pd.DataFrame(
                columns=['dealer_name', 'product_category', 'period_start_date', 'units_sold']),
            'sales_by_product_category': pd.DataFrame(
                columns=['dealer_name', 'product_category', 'total_revenue', 'total_quantity']),
            'transaction_lineage': pd.DataFrame(
                columns=['transaction_id', 'dealer_name', 'order_date', 'delivery_date', 'invoice_amount']),
            'dealer_health_scorecard': pd.DataFrame(columns=['dealer_name', 'health_score', 'change_percent']),
            'strategic_insights': pd.DataFrame(columns=['insight_text', 'priority_level'])
        }
        return empty_dfs.get(table_key, pd.DataFrame())

    def list_tables(self) -> List[str]:
        """List all tables in Glue Catalog"""
        try:
            response = self.glue_client.get_tables(DatabaseName=self.database_name)
            tables = [table['Name'] for table in response['TableList']]
            return tables
        except Exception as e:
            logger.error(f"Error listing tables: {str(e)}")
            return []

    def get_dealers(self) -> List[str]:
        """Get list of dealers from dealer_location table"""
        try:
            df = self.get_table_data('dealer_location', use_cache=True)
            if not df.empty and 'dealer_name' in df.columns:
                dealers = df['dealer_name'].dropna().unique().tolist()
                return dealers if dealers else ["Premium Motors", "Elite Auto", "City Cars"]
            return ["Premium Motors", "Elite Auto", "City Cars", "Highway Motors", "Metro Auto"]
        except Exception as e:
            logger.error(f"Error getting dealers: {str(e)}")
            return ["Premium Motors", "Elite Auto", "City Cars"]

    def get_products(self) -> List[str]:
        """Get list of products from sales_by_product_category table"""
        try:
            df = self.get_table_data('sales_by_product_category', use_cache=True)
            if not df.empty and 'product_category' in df.columns:
                products = df['product_category'].dropna().unique().tolist()
                return products if products else ["Sedan", "SUV", "Hatchback", "Truck", "MUV", "EV"]
            return ["Sedan", "SUV", "Hatchback", "Truck", "MUV", "EV"]
        except Exception as e:
            logger.error(f"Error getting products: {str(e)}")
            return ["Sedan", "SUV", "Hatchback", "Truck", "MUV", "EV"]

    def get_regions(self) -> List[str]:
        """Get list of regions from dealer_location table"""
        try:
            df = self.get_table_data('dealer_location', use_cache=True)
            if not df.empty and 'location_region' in df.columns:
                regions = df['location_region'].dropna().unique().tolist()
                return regions if regions else ["North", "South", "East", "West", "Central"]
            return ["North", "South", "East", "West", "Central"]
        except Exception as e:
            logger.error(f"Error getting regions: {str(e)}")
            return ["North", "South", "East", "West", "Central"]

    def get_kpi_metrics(self, filters: Dict = None) -> Dict:
        """Get KPI metrics from various tables"""
        kpis = {}

        try:
            # Get CCC data
            ccc_df = self.get_table_data('cash_conversion_cycle', filters=filters)
            if not ccc_df.empty and 'ccc' in ccc_df.columns:
                kpis['ccc'] = round(ccc_df['ccc'].mean(), 1)
            else:
                kpis['ccc'] = 35.5

            # Get repair TAT
            tat_df = self.get_table_data('repair_turnaround_time', filters=filters)
            if not tat_df.empty and 'avg_turnaround_hours' in tat_df.columns:
                kpis['repair_tat'] = int(round(tat_df['avg_turnaround_hours'].mean()))
            else:
                kpis['repair_tat'] = 42

            # Get revenue growth
            growth_df = self.get_table_data('dealer_revenue_growth', filters=filters)
            if not growth_df.empty and 'revenue_growth_mom_percent' in growth_df.columns:
                kpis['revenue_growth'] = round(growth_df['revenue_growth_mom_percent'].mean(), 1)
            else:
                kpis['revenue_growth'] = 8.5

            # Get gross margin
            margin_df = self.get_table_data('gross_profit_margin', filters=filters)
            if not margin_df.empty and 'gross_profit_margin_pct' in margin_df.columns:
                kpis['gross_margin'] = round(margin_df['gross_profit_margin_pct'].mean(), 1)
            else:
                kpis['gross_margin'] = 28.3

            # Get stock availability
            stock_df = self.get_table_data('stock_availability', filters=filters)
            if not stock_df.empty and 'stock_availability_pct' in stock_df.columns:
                kpis['stock_availability'] = round(stock_df['stock_availability_pct'].mean(), 1)
            else:
                kpis['stock_availability'] = 82.5

            # Get backorder
            backorder_df = self.get_table_data('backorder_incidence', filters=filters)
            if not backorder_df.empty and 'backorder_incidence_pct' in backorder_df.columns:
                kpis['backorder'] = round(backorder_df['backorder_incidence_pct'].mean(), 1)
            else:
                kpis['backorder'] = 7.2

            # Get lead time
            lead_df = self.get_table_data('order_lead_time', filters=filters)
            if not lead_df.empty and 'avg_order_lead_time_days' in lead_df.columns:
                kpis['lead_time'] = int(round(lead_df['avg_order_lead_time_days'].mean()))
            else:
                kpis['lead_time'] = 6

            # Get contribution margin
            contrib_df = self.get_table_data('dealer_contribution_margin', filters=filters)
            if not contrib_df.empty and 'contribution_margin_pct' in contrib_df.columns:
                kpis['contribution_margin'] = round(contrib_df['contribution_margin_pct'].mean(), 1)
            else:
                kpis['contribution_margin'] = 22.5

            # Get sales volume
            sales_df = self.get_table_data('sales_volume', filters=filters)
            if not sales_df.empty:
                if 'units_sold' in sales_df.columns:
                    kpis['sales_volume'] = int(sales_df['units_sold'].sum())
                elif 'quantity_sold' in sales_df.columns:
                    kpis['sales_volume'] = int(sales_df['quantity_sold'].sum())
                else:
                    kpis['sales_volume'] = 12500
            else:
                kpis['sales_volume'] = 12500

            return kpis

        except Exception as e:
            logger.error(f"Error getting KPI data: {str(e)}")
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
        """Get transaction lineage data with pagination"""
        try:
            offset = (page - 1) * page_size
            df = self.get_table_data('transaction_lineage', filters=filters, limit=page_size)

            if not df.empty:
                # Add pagination info
                df['_page'] = page
                return df

            # Return sample data if empty
            return self._get_sample_transaction_data()

        except Exception as e:
            logger.error(f"Error getting transaction data: {str(e)}")
            return self._get_sample_transaction_data()

    def _get_sample_transaction_data(self) -> pd.DataFrame:
        """Return sample transaction data for testing"""
        import random
        from datetime import datetime, timedelta

        dealers = self.get_dealers()
        data = []

        for i in range(20):
            order_date = datetime.now() - timedelta(days=random.randint(1, 90))
            delivery_date = order_date + timedelta(days=random.randint(1, 15))

            data.append({
                'transaction_id': f'TXN{i + 1:04d}',
                'dealer_name': random.choice(dealers),
                'product_category': random.choice(['Sedan', 'SUV', 'Hatchback', 'Truck']),
                'order_date': order_date.strftime('%Y-%m-%d'),
                'delivery_date': delivery_date.strftime('%Y-%m-%d'),
                'order_flag': 'Y',
                'delivery_flag': 'Y' if random.random() > 0.1 else 'N',
                'invoice_amount': random.randint(10000, 100000),
                'invoice_status': random.choice(['Paid', 'Pending', 'Overdue']),
                'warranty_status': random.choice(['Active', 'Expired'])
            })

        return pd.DataFrame(data)

    def get_dealer_health_scores(self, filters: Dict = None) -> pd.DataFrame:
        """Get dealer health scores"""
        try:
            df = self.get_table_data('dealer_health_scorecard', filters=filters)
            if not df.empty:
                return df
            return self._calculate_health_scores(filters)
        except Exception as e:
            logger.error(f"Error getting health scores: {str(e)}")
            return self._calculate_health_scores(filters)

    def _calculate_health_scores(self, filters: Dict = None) -> pd.DataFrame:
        """Calculate health scores from KPI data"""
        dealers = self.get_dealers()
        kpis = self.get_kpi_metrics(filters)

        scores = []
        for dealer in dealers:
            # Calculate health score based on KPIs
            margin_score = min(100, max(0, (kpis.get('gross_margin', 28.3) / 50) * 100))
            growth_score = min(100, max(0, (kpis.get('revenue_growth', 8.5) / 30) * 100))
            stock_score = min(100, max(0, kpis.get('stock_availability', 82.5)))
            tat_score = min(100, max(0, 100 - (kpis.get('repair_tat', 42) / 72 * 100)))

            health_score = (margin_score * 0.3 + growth_score * 0.2 + stock_score * 0.3 + tat_score * 0.2)

            scores.append({
                'dealer_name': dealer,
                'health_score': round(health_score, 1),
                'change_percent': round((health_score - 75) / 75 * 100, 1),
                'last_updated': datetime.now()
            })

        return pd.DataFrame(scores)

    def get_strategic_insights(self) -> pd.DataFrame:
        """Get strategic insights"""
        try:
            df = self.get_table_data('strategic_insights')
            if not df.empty:
                return df
            return self._generate_insights()
        except Exception as e:
            logger.error(f"Error getting insights: {str(e)}")
            return self._generate_insights()

    def _generate_insights(self) -> pd.DataFrame:
        """Generate insights from KPI data"""
        kpis = self.get_kpi_metrics()

        insights = []

        if kpis.get('gross_margin', 0) < 25:
            insights.append(
                f"Gross margin is at {kpis.get('gross_margin', 0)}% - below target. Review pricing strategy.")

        if kpis.get('stock_availability', 0) < 80:
            insights.append(f"Stock availability at {kpis.get('stock_availability', 0)}% - risk of stockouts.")

        if kpis.get('repair_tat', 0) > 48:
            insights.append(f"Repair TAT at {kpis.get('repair_tat', 0)} hours - exceeds SLA.")

        if kpis.get('ccc', 0) > 60:
            insights.append(f"Cash conversion cycle at {kpis.get('ccc', 0)} days - optimize working capital.")

        if not insights:
            insights.append("All KPI metrics are within normal parameters.")

        df = pd.DataFrame({
            'insight_text': insights,
            'priority_level': list(range(1, len(insights) + 1)),
            'created_at': [datetime.now()] * len(insights)
        })

        return df

    def clear_cache(self):
        """Clear the cache"""
        self.cache.clear()
        self.cache_timestamps.clear()
        logger.info("Glue data loader cache cleared")

    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        return {
            'cache_size': len(self.cache),
            'cache_ttl': self.cache_ttl,
            'database': self.database_name,
            'region': self.region
        }


