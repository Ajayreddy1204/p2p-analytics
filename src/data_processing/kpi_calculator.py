"""
KPI Calculator Module
Calculates all dealer performance KPIs from raw data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging

logger = logging.getLogger(__name__)


class KPICalculator:
    """Calculate and aggregate KPI metrics for dealer performance"""

    def __init__(self, db_connection):
        """
        Initialize KPI Calculator with database connection

        Args:
            db_connection: Database connection object (Redshift or Aurora)
        """
        self.db = db_connection
        self.logger = logging.getLogger(__name__)

    def calculate_cash_conversion_cycle(self, filters: Dict = None) -> Optional[float]:
        """
        Calculate Cash Conversion Cycle (CCC) in days
        CCC = DSO + DIO - DPO

        Args:
            filters: Dictionary with dealer, date_range filters

        Returns:
            CCC value in days or None if insufficient data
        """
        try:
            query = """
            SELECT 
                AVG(DSO) AS avg_dso,
                AVG(DIO) AS avg_dio,
                AVG(DPO) AS avg_dpo
            FROM dealer_information_mart.vw_cash_conversion_cycle
            WHERE 1=1
            """

            if filters and filters.get('dealer') and filters['dealer'] != 'All Dealers':
                query += f" AND dealer_name = '{filters['dealer']}'"

            if filters and filters.get('from_date') and filters.get('to_date'):
                query += f" AND period_start_date >= '{filters['from_date']}'"
                query += f" AND period_start_date <= '{filters['to_date']}'"

            result = self.db.execute_query(query)

            if result and not result.empty:
                dso = result['avg_dso'].iloc[0] if result['avg_dso'].iloc[0] else 0
                dio = result['avg_dio'].iloc[0] if result['avg_dio'].iloc[0] else 0
                dpo = result['avg_dpo'].iloc[0] if result['avg_dpo'].iloc[0] else 0
                ccc = dso + dio - dpo
                return round(ccc, 1) if ccc else None

            return None

        except Exception as e:
            self.logger.error(f"Error calculating CCC: {str(e)}")
            return None

    def calculate_repair_turnaround_time(self, filters: Dict = None) -> Optional[int]:
        """
        Calculate Average Repair Turnaround Time in hours

        Args:
            filters: Dictionary with dealer, date_range filters

        Returns:
            Average TAT in hours or None
        """
        try:
            query = """
            SELECT AVG(avg_turnaround_hours) as avg_tat
            FROM dealer_information_mart.vw_average_repair_turnaround_time
            WHERE avg_turnaround_hours IS NOT NULL
            """

            if filters and filters.get('dealer') and filters['dealer'] != 'All Dealers':
                query += f" AND dealer_name = '{filters['dealer']}'"

            if filters and filters.get('from_date') and filters.get('to_date'):
                query += f" AND period_start_date >= '{filters['from_date']}'"
                query += f" AND period_start_date <= '{filters['to_date']}'"

            result = self.db.execute_query(query)

            if result and not result.empty:
                tat = result['avg_tat'].iloc[0]
                if pd.notna(tat):
                    return int(round(tat))

            return None

        except Exception as e:
            self.logger.error(f"Error calculating TAT: {str(e)}")
            return None

    def calculate_revenue_growth(self, filters: Dict = None) -> Optional[float]:
        """
        Calculate Revenue Growth percentage (MoM)

        Args:
            filters: Dictionary with dealer, date_range filters

        Returns:
            Revenue growth percentage or None
        """
        try:
            query = """
            SELECT AVG(revenue_growth_mom_percent) as avg_growth
            FROM dealer_information_mart.vw_dealer_revenue_growth
            WHERE revenue_growth_mom_percent IS NOT NULL
            """

            if filters and filters.get('dealer') and filters['dealer'] != 'All Dealers':
                query += f" AND dealer_name = '{filters['dealer']}'"

            if filters and filters.get('from_date') and filters.get('to_date'):
                query += f" AND period_month >= DATE_TRUNC('month', '{filters['from_date']}')"
                query += f" AND period_month <= DATE_TRUNC('month', '{filters['to_date']}')"

            result = self.db.execute_query(query)

            if result and not result.empty:
                growth = result['avg_growth'].iloc[0]
                if pd.notna(growth):
                    return round(growth, 1)

            return None

        except Exception as e:
            self.logger.error(f"Error calculating revenue growth: {str(e)}")
            return None

    def calculate_gross_profit_margin(self, filters: Dict = None) -> Optional[float]:
        """
        Calculate Gross Profit Margin percentage

        Args:
            filters: Dictionary with dealer, date_range filters

        Returns:
            Gross profit margin percentage or None
        """
        try:
            query = """
            SELECT AVG(gross_profit_margin_pct) as avg_margin
            FROM dealer_information_mart.vw_gross_profit_margin
            WHERE gross_profit_margin_pct IS NOT NULL
            """

            if filters and filters.get('dealer') and filters['dealer'] != 'All Dealers':
                query += f" AND dealer_name = '{filters['dealer']}'"

            if filters and filters.get('from_date') and filters.get('to_date'):
                query += f" AND period_month >= DATE_TRUNC('month', '{filters['from_date']}')"
                query += f" AND period_month <= DATE_TRUNC('month', '{filters['to_date']}')"

            result = self.db.execute_query(query)

            if result and not result.empty:
                margin = result['avg_margin'].iloc[0]
                if pd.notna(margin):
                    return round(margin, 1)

            return None

        except Exception as e:
            self.logger.error(f"Error calculating gross margin: {str(e)}")
            return None

    def calculate_contribution_margin(self, filters: Dict = None) -> Optional[float]:
        """
        Calculate Contribution Margin percentage

        Args:
            filters: Dictionary with dealer, date_range filters

        Returns:
            Contribution margin percentage or None
        """
        try:
            query = """
            SELECT AVG(contribution_margin_pct) as avg_contrib
            FROM dealer_information_mart.vw_dealer_contribution_margin
            WHERE contribution_margin_pct IS NOT NULL
            """

            if filters and filters.get('dealer') and filters['dealer'] != 'All Dealers':
                query += f" AND dealer_name = '{filters['dealer']}'"

            if filters and filters.get('from_date') and filters.get('to_date'):
                query += f" AND period_start_date >= '{filters['from_date']}'"
                query += f" AND period_start_date <= '{filters['to_date']}'"

            result = self.db.execute_query(query)

            if result and not result.empty:
                margin = result['avg_contrib'].iloc[0]
                if pd.notna(margin):
                    return round(margin, 1)

            return None

        except Exception as e:
            self.logger.error(f"Error calculating contribution margin: {str(e)}")
            return None

    def calculate_stock_availability(self, filters: Dict = None) -> Optional[float]:
        """
        Calculate Stock Availability percentage

        Args:
            filters: Dictionary with dealer, date_range filters

        Returns:
            Stock availability percentage or None
        """
        try:
            query = """
            SELECT AVG(stock_availability_pct) as avg_avail
            FROM dealer_information_mart.vw_stock_availability_dealer
            WHERE stock_availability_pct IS NOT NULL
            """

            if filters and filters.get('dealer') and filters['dealer'] != 'All Dealers':
                query += f" AND dealer_name = '{filters['dealer']}'"

            if filters and filters.get('from_date') and filters.get('to_date'):
                query += f" AND period_start_date >= '{filters['from_date']}'"
                query += f" AND period_start_date <= '{filters['to_date']}'"

            result = self.db.execute_query(query)

            if result and not result.empty:
                avail = result['avg_avail'].iloc[0]
                if pd.notna(avail):
                    return round(avail, 1)

            return None

        except Exception as e:
            self.logger.error(f"Error calculating stock availability: {str(e)}")
            return None

    def calculate_backorder_incidence(self, filters: Dict = None) -> Optional[float]:
        """
        Calculate Backorder Incidence percentage

        Args:
            filters: Dictionary with dealer, date_range filters

        Returns:
            Backorder incidence percentage or None
        """
        try:
            query = """
            SELECT AVG(backorder_incidence_pct) as avg_backorder
            FROM dealer_information_mart.vw_backorder_incidence
            WHERE backorder_incidence_pct IS NOT NULL
            """

            if filters and filters.get('dealer') and filters['dealer'] != 'All Dealers':
                query += f" AND dealer_name = '{filters['dealer']}'"

            if filters and filters.get('from_date') and filters.get('to_date'):
                query += f" AND period_start_date >= '{filters['from_date']}'"
                query += f" AND period_start_date <= '{filters['to_date']}'"

            result = self.db.execute_query(query)

            if result and not result.empty:
                backorder = result['avg_backorder'].iloc[0]
                if pd.notna(backorder):
                    return round(backorder, 1)

            return None

        except Exception as e:
            self.logger.error(f"Error calculating backorder incidence: {str(e)}")
            return None

    def calculate_order_lead_time(self, filters: Dict = None) -> Optional[int]:
        """
        Calculate Order Lead Time in days

        Args:
            filters: Dictionary with dealer, date_range filters

        Returns:
            Order lead time in days or None
        """
        try:
            query = """
            SELECT AVG(avg_order_lead_time_days) as avg_lead
            FROM dealer_information_mart.vw_order_lead_time
            WHERE avg_order_lead_time_days IS NOT NULL
            """

            if filters and filters.get('dealer') and filters['dealer'] != 'All Dealers':
                query += f" AND dealer_name = '{filters['dealer']}'"

            if filters and filters.get('from_date') and filters.get('to_date'):
                query += f" AND period_start_date >= '{filters['from_date']}'"
                query += f" AND period_start_date <= '{filters['to_date']}'"

            result = self.db.execute_query(query)

            if result and not result.empty:
                lead = result['avg_lead'].iloc[0]
                if pd.notna(lead):
                    return int(round(lead))

            return None

        except Exception as e:
            self.logger.error(f"Error calculating order lead time: {str(e)}")
            return None

    def calculate_sales_volume(self, filters: Dict = None) -> Optional[int]:
        """
        Calculate Sales Volume (total units sold)

        Args:
            filters: Dictionary with dealer, date_range filters

        Returns:
            Total units sold or None
        """
        try:
            query = """
            SELECT SUM(units_sold) as total_units
            FROM dealer_information_mart.vw_sales_volume
            WHERE 1=1
            """

            if filters and filters.get('dealer') and filters['dealer'] != 'All Dealers':
                query += f" AND dealer_name = '{filters['dealer']}'"

            if filters and filters.get('product') and filters['product']:
                query += f" AND product_category = '{filters['product']}'"

            if filters and filters.get('from_date') and filters.get('to_date'):
                query += f" AND period_start_date >= '{filters['from_date']}'"
                query += f" AND period_start_date <= '{filters['to_date']}'"

            result = self.db.execute_query(query)

            if result and not result.empty:
                units = result['total_units'].iloc[0]
                if pd.notna(units):
                    return int(units)

            return None

        except Exception as e:
            self.logger.error(f"Error calculating sales volume: {str(e)}")
            return None

    def calculate_dealer_health_score(self, dealer_name: str, filters: Dict = None) -> Dict:
        """
        Calculate comprehensive health score for a dealer

        Args:
            dealer_name: Name of the dealer
            filters: Additional filters (date range, etc.)

        Returns:
            Dictionary with health score and individual metric scores
        """
        try:
            # Apply dealer filter
            dealer_filters = filters.copy() if filters else {}
            dealer_filters['dealer'] = dealer_name

            # Calculate all metrics
            metrics = {
                'revenue_growth': self.calculate_revenue_growth(dealer_filters),
                'gross_margin': self.calculate_gross_profit_margin(dealer_filters),
                'contribution_margin': self.calculate_contribution_margin(dealer_filters),
                'stock_availability': self.calculate_stock_availability(dealer_filters),
                'backorder': self.calculate_backorder_incidence(dealer_filters),
                'ccc': self.calculate_cash_conversion_cycle(dealer_filters),
                'repair_tat': self.calculate_repair_turnaround_time(dealer_filters),
                'order_lead_time': self.calculate_order_lead_time(dealer_filters),
                'sales_volume': self.calculate_sales_volume(dealer_filters)
            }

            # Score each metric (1-7 scale)
            scores = {
                'revenue_growth': self._score_revenue_growth(metrics['revenue_growth']),
                'gross_margin': self._score_gross_margin(metrics['gross_margin']),
                'contribution_margin': self._score_contribution_margin(metrics['contribution_margin']),
                'stock_availability': self._score_stock_availability(metrics['stock_availability']),
                'backorder': self._score_backorder(metrics['backorder']),
                'ccc': self._score_ccc(metrics['ccc']),
                'repair_tat': self._score_repair_tat(metrics['repair_tat']),
                'order_lead_time': self._score_lead_time(metrics['order_lead_time']),
                'sales_volume': self._score_sales_volume(metrics['sales_volume'])
            }

            # Calculate overall score (exclude None values)
            valid_scores = [s for s in scores.values() if s is not None]
            if valid_scores:
                overall_score = sum(valid_scores) / len(valid_scores)
            else:
                overall_score = 0

            # Determine risk level
            if overall_score <= 2:
                risk_level = 'critical'
            elif overall_score <= 4:
                risk_level = 'average'
            elif overall_score <= 5.9:
                risk_level = 'good'
            else:
                risk_level = 'excellent'

            return {
                'dealer_name': dealer_name,
                'overall_score': round(overall_score, 1),
                'risk_level': risk_level,
                'metrics': metrics,
                'scores': scores,
                'timestamp': datetime.now()
            }

        except Exception as e:
            self.logger.error(f"Error calculating health score for {dealer_name}: {str(e)}")
            return {
                'dealer_name': dealer_name,
                'overall_score': 0,
                'risk_level': 'unknown',
                'error': str(e)
            }

    def _score_revenue_growth(self, value: float) -> Optional[int]:
        """Score revenue growth on 1-7 scale (higher is better)"""
        if value is None:
            return None
        if value < 0:
            return 1
        if value < 2:
            return 2
        if value < 4:
            return 3
        if value < 6:
            return 4
        if value < 8:
            return 5
        if value < 12:
            return 6
        return 7

    def _score_gross_margin(self, value: float) -> Optional[int]:
        """Score gross margin on 1-7 scale (higher is better)"""
        if value is None:
            return None
        if value < 10:
            return 1
        if value < 20:
            return 2
        if value < 30:
            return 3
        if value < 40:
            return 4
        if value < 55:
            return 5
        if value < 65:
            return 6
        return 7

    def _score_contribution_margin(self, value: float) -> Optional[int]:
        """Score contribution margin on 1-7 scale (higher is better)"""
        if value is None:
            return None
        if value < 5:
            return 1
        if value < 15:
            return 2
        if value < 25:
            return 3
        if value < 35:
            return 4
        if value < 45:
            return 5
        if value < 55:
            return 6
        return 7

    def _score_stock_availability(self, value: float) -> Optional[int]:
        """Score stock availability on 1-7 scale (higher is better)"""
        if value is None:
            return None
        if value < 40:
            return 1
        if value < 55:
            return 2
        if value < 65:
            return 3
        if value < 75:
            return 4
        if value < 85:
            return 5
        if value < 93:
            return 6
        return 7

    def _score_backorder(self, value: float) -> Optional[int]:
        """Score backorder incidence on 1-7 scale (lower is better)"""
        if value is None:
            return None
        if value > 20:
            return 1
        if value > 15:
            return 2
        if value > 10:
            return 3
        if value > 7:
            return 4
        if value > 4:
            return 5
        if value > 2:
            return 6
        return 7

    def _score_ccc(self, value: float) -> Optional[int]:
        """Score cash conversion cycle on 1-7 scale (lower is better)"""
        if value is None:
            return None
        if value > 60:
            return 1
        if value > 45:
            return 2
        if value > 30:
            return 3
        if value > 20:
            return 4
        if value > 12:
            return 5
        if value > 6:
            return 6
        return 7

    def _score_repair_tat(self, value: float) -> Optional[int]:
        """Score repair TAT on 1-7 scale (lower is better)"""
        if value is None:
            return None
        if value > 72:
            return 1
        if value > 60:
            return 2
        if value > 48:
            return 3
        if value > 36:
            return 4
        if value > 24:
            return 5
        if value > 12:
            return 6
        return 7

    def _score_lead_time(self, value: float) -> Optional[int]:
        """Score order lead time on 1-7 scale (lower is better)"""
        if value is None:
            return None
        if value > 10:
            return 1
        if value > 8:
            return 2
        if value > 6:
            return 3
        if value > 4:
            return 4
        if value > 2:
            return 5
        if value > 1:
            return 6
        return 7

    def _score_sales_volume(self, value: float) -> Optional[int]:
        """Score sales volume on 1-7 scale"""
        if value is None:
            return None
        # Use percentile-based scoring (simplified version)
        if value > 10000:
            return 7
        if value > 5000:
            return 6
        if value > 2500:
            return 5
        if value > 1000:
            return 4
        if value > 500:
            return 3
        if value > 100:
            return 2
        return 1

    def get_top_dealers_by_metric(self, metric: str, limit: int = 5) -> List[Dict]:
        """
        Get top dealers by a specific metric

        Args:
            metric: Metric name (revenue_growth, gross_margin, etc.)
            limit: Number of dealers to return

        Returns:
            List of dealer metric dictionaries
        """
        try:
            metric_map = {
                'revenue_growth': ('vw_dealer_revenue_growth', 'revenue_growth_mom_percent'),
                'gross_margin': ('vw_gross_profit_margin', 'gross_profit_margin_pct'),
                'contribution_margin': ('vw_dealer_contribution_margin', 'contribution_margin_pct'),
                'stock_availability': ('vw_stock_availability_dealer', 'stock_availability_pct'),
                'sales_volume': ('vw_sales_volume', 'units_sold')
            }

            if metric not in metric_map:
                return []

            table, column = metric_map[metric]

            query = f"""
            SELECT dealer_name, AVG({column}) as metric_value
            FROM dealer_information_mart.{table}
            WHERE {column} IS NOT NULL
            GROUP BY dealer_name
            ORDER BY metric_value DESC
            LIMIT {limit}
            """

            result = self.db.execute_query(query)

            if result and not result.empty:
                return result.to_dict('records')

            return []

        except Exception as e:
            self.logger.error(f"Error getting top dealers for {metric}: {str(e)}")
            return []

    def get_bottom_dealers_by_metric(self, metric: str, limit: int = 5) -> List[Dict]:
        """
        Get bottom dealers by a specific metric

        Args:
            metric: Metric name (revenue_growth, gross_margin, etc.)
            limit: Number of dealers to return

        Returns:
            List of dealer metric dictionaries
        """
        try:
            metric_map = {
                'revenue_growth': ('vw_dealer_revenue_growth', 'revenue_growth_mom_percent'),
                'gross_margin': ('vw_gross_profit_margin', 'gross_profit_margin_pct'),
                'contribution_margin': ('vw_dealer_contribution_margin', 'contribution_margin_pct'),
                'stock_availability': ('vw_stock_availability_dealer', 'stock_availability_pct'),
                'backorder': ('vw_backorder_incidence', 'backorder_incidence_pct'),
                'ccc': ('vw_cash_conversion_cycle', 'ccc')
            }

            if metric not in metric_map:
                return []

            table, column = metric_map[metric]

            # For metrics where lower is worse, order ASC
            query = f"""
            SELECT dealer_name, AVG({column}) as metric_value
            FROM dealer_information_mart.{table}
            WHERE {column} IS NOT NULL
            GROUP BY dealer_name
            ORDER BY metric_value ASC
            LIMIT {limit}
            """

            result = self.db.execute_query(query)

            if result and not result.empty:
                return result.to_dict('records')

            return []

        except Exception as e:
            self.logger.error(f"Error getting bottom dealers for {metric}: {str(e)}")
            return []