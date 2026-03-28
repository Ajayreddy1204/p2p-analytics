"""
Data Validator Module
Validates input data and ensures required columns exist
"""

import pandas as pd
import re
from typing import Dict, List, Optional, Tuple, Any
import logging

logger = logging.getLogger(__name__)


class DataValidator:
    """Validate data quality and schema requirements"""

    def __init__(self):
        """Initialize data validator"""
        self.logger = logging.getLogger(__name__)

        # Required views and their expected columns
        self.required_views = {
            'vw_dealer_revenue_growth': ['dealer_name', 'revenue_growth_mom_percent', 'period_month'],
            'vw_gross_profit_margin': ['dealer_name', 'gross_profit_margin_pct', 'total_revenue'],
            'vw_cash_conversion_cycle': ['dealer_name', 'dso', 'dio', 'dpo', 'ccc'],
            'vw_average_repair_turnaround_time': ['dealer_name', 'avg_turnaround_hours'],
            'vw_order_lead_time': ['dealer_name', 'avg_order_lead_time_days'],
            'vw_stock_availability_dealer': ['dealer_name', 'stock_availability_pct'],
            'vw_backorder_incidence': ['dealer_name', 'backorder_incidence_pct'],
            'vw_dealer_contribution_margin': ['dealer_name', 'contribution_margin_pct'],
            'vw_sales_volume': ['dealer_name', 'units_sold', 'product_category'],
            'vw_sales_per_product_category': ['dealer_name', 'product_category', 'total_revenue', 'total_quantity']
        }

    def validate_schema(self, db_connection) -> Dict[str, List[str]]:
        """
        Validate that all required views exist with required columns

        Args:
            db_connection: Database connection object

        Returns:
            Dictionary of missing views and missing columns
        """
        missing_views = []
        missing_columns = {}

        for view_name, required_cols in self.required_views.items():
            try:
                # Check if view exists
                query = f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_name = '{view_name}'
                ) as exists_flag
                """
                result = db_connection.execute_query(query)

                if result and result['exists_flag'].iloc[0]:
                    # Get actual columns
                    col_query = f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{view_name}'
                    """
                    col_result = db_connection.execute_query(col_query)
                    actual_cols = set(col_result['column_name'].str.lower().tolist())

                    # Check for missing columns
                    missing = [col for col in required_cols if col not in actual_cols]
                    if missing:
                        missing_columns[view_name] = missing
                else:
                    missing_views.append(view_name)

            except Exception as e:
                self.logger.error(f"Error validating {view_name}: {str(e)}")
                missing_views.append(view_name)

        return {
            'missing_views': missing_views,
            'missing_columns': missing_columns
        }

    def validate_filters(self, filters: Dict) -> Tuple[bool, List[str]]:
        """
        Validate filter parameters

        Args:
            filters: Dictionary of filter parameters

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []

        # Validate date range
        if filters.get('from_date') and filters.get('to_date'):
            if filters['from_date'] > filters['to_date']:
                errors.append("From date cannot be after to date")

        # Validate dealer name (if provided)
        if filters.get('dealer') and filters['dealer'] != 'All Dealers':
            if not re.match(r'^[a-zA-Z0-9\s\-_]+$', filters['dealer']):
                errors.append("Dealer name contains invalid characters")

        # Validate product category
        if filters.get('product') and filters['product'] != 'Product':
            if not re.match(r'^[a-zA-Z0-9\s\-_]+$', filters['product']):
                errors.append("Product category contains invalid characters")

        # Validate metric
        valid_metrics = ['Revenue', 'Units', 'Margin', 'Growth', 'CCC']
        if filters.get('metric') and filters['metric'] not in valid_metrics:
            errors.append(f"Invalid metric. Must be one of: {', '.join(valid_metrics)}")

        # Validate time period
        valid_periods = ['Last 30 Days', 'QTD', 'YTD', 'All Dates']
        if filters.get('time_period') and filters['time_period'] not in valid_periods:
            errors.append(f"Invalid time period. Must be one of: {', '.join(valid_periods)}")

        return len(errors) == 0, errors

    def validate_sql(self, sql: str) -> Tuple[bool, str]:
        """
        Validate SQL for safety and correctness

        Args:
            sql: SQL query string

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not sql or not sql.strip():
            return False, "Empty SQL statement"

        sql_upper = sql.upper()

        # Check for dangerous operations
        dangerous_ops = ['DROP', 'DELETE', 'TRUNCATE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE']
        for op in dangerous_ops:
            if op in sql_upper:
                return False, f"Operation {op} is not allowed in queries"

        # Check for SELECT statement
        if 'SELECT' not in sql_upper:
            return False, "Query must be a SELECT statement"

        # Check for FROM clause
        if 'FROM' not in sql_upper:
            return False, "Missing FROM clause"

        # Check for ambiguous column references (basic check)
        if 'DEALER_NAME = DEALER_NAME' in sql_upper:
            return False, "Ambiguous column reference: DEALER_NAME = DEALER_NAME without table aliases"

        # Check for common column name errors
        common_errors = {
            'AVG_HOURS': 'AVG_TURNAROUND_HOURS',
            'MARGIN_PCT': 'GROSS_PROFIT_MARGIN_PCT',
            'LEAD_TIME_DAYS': 'AVG_ORDER_LEAD_TIME_DAYS',
            'CASH_CYCLE': 'CCC'
        }

        for wrong, correct in common_errors.items():
            if wrong in sql_upper:
                return False, f"Wrong column name: {wrong} (should be {correct})"

        return True, ""

    def validate_dataframe(self, df: pd.DataFrame, min_rows: int = 1) -> Tuple[bool, str]:
        """
        Validate DataFrame for quality and completeness

        Args:
            df: Pandas DataFrame to validate
            min_rows: Minimum number of rows required

        Returns:
            Tuple of (is_valid, message)
        """
        if df is None:
            return False, "DataFrame is None"

        if df.empty:
            return False, f"DataFrame is empty (needs at least {min_rows} rows)"

        if len(df) < min_rows:
            return False, f"DataFrame has only {len(df)} rows, needs at least {min_rows}"

        # Check for too many rows (safety limit)
        max_rows = 50000
        if len(df) > max_rows:
            return False, f"DataFrame has {len(df)} rows, exceeds limit of {max_rows}"

        # Check for null values in key columns
        for col in df.columns:
            null_pct = df[col].isnull().mean() * 100
            if null_pct > 50:
                return False, f"Column {col} has {null_pct:.1f}% null values (exceeds 50% threshold)"

        return True, "Valid"

    def validate_question(self, question: str) -> Tuple[bool, str]:
        """
        Validate user question for safety and appropriateness

        Args:
            question: User question string

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not question or not question.strip():
            return False, "Question cannot be empty"

        if len(question) > 1000:
            return False, "Question exceeds maximum length of 1000 characters"

        # Check for potentially harmful content
        dangerous_patterns = [
            r'DROP\s+TABLE',
            r'DELETE\s+FROM',
            r'INSERT\s+INTO',
            r'UPDATE\s+SET',
            r'ALTER\s+TABLE',
            r'CREATE\s+TABLE',
            r'TRUNCATE\s+TABLE',
            r'GRANT\s+TO',
            r'REVOKE\s+FROM'
        ]

        question_upper = question.upper()
        for pattern in dangerous_patterns:
            if re.search(pattern, question_upper):
                return False, f"Question contains disallowed content: {pattern}"

        return True, ""