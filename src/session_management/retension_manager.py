"""
Retention Manager Module
Manages data lifecycle and retention policies
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class RetentionManager:
    """Manage data retention and cleanup"""

    def __init__(self, db_connection):
        """
        Initialize retention manager

        Args:
            db_connection: Database connection object
        """
        self.db = db_connection
        self.default_retention_days = {
            'user_sessions': 30,
            'chat_history': 90,
            'query_cache': 7,
            'audit_logs': 365
        }

    def set_retention_policy(self, table: str, days: int) -> bool:
        """
        Set retention policy for a table

        Args:
            table: Table name
            days: Number of days to retain data

        Returns:
            Success flag
        """
        try:
            self.default_retention_days[table] = days
            logger.info(f"Set retention policy for {table}: {days} days")
            return True

        except Exception as e:
            logger.error(f"Error setting retention policy: {str(e)}")
            return False

    def cleanup_table(self, table: str, days: int = None) -> int:
        """
        Clean up old records from a table

        Args:
            table: Table name
            days: Retention days (uses default if not specified)

        Returns:
            Number of records deleted
        """
        try:
            if days is None:
                days = self.default_retention_days.get(table, 30)

            cutoff_date = datetime.now() - timedelta(days=days)

            # Map table to date column
            date_columns = {
                'user_sessions': 'expires_at',
                'chat_history': 'created_at',
                'query_cache': 'expires_at'
            }

            date_col = date_columns.get(table, 'created_at')

            result = self.db.execute_query(f"""
            DELETE FROM {table}
            WHERE {date_col} < %s
            RETURNING id
            """, (cutoff_date,))

            deleted_count = len(result) if result else 0
            logger.info(f"Cleaned up {deleted_count} records from {table}")

            return deleted_count

        except Exception as e:
            logger.error(f"Error cleaning up {table}: {str(e)}")
            return 0

    def cleanup_all(self) -> Dict[str, int]:
        """
        Clean up all tables based on retention policies

        Returns:
            Dictionary with cleanup counts
        """
        results = {}

        for table in self.default_retention_days.keys():
            if table in ['user_sessions', 'chat_history', 'query_cache']:
                results[table] = self.cleanup_table(table)

        return results

    def archive_old_data(self, table: str, days: int = 90, archive_table: str = None) -> bool:
        """
        Archive old data before deletion

        Args:
            table: Source table
            days: Days threshold for archiving
            archive_table: Destination table (creates if not exists)

        Returns:
            Success flag
        """
        try:
            if archive_table is None:
                archive_table = f"{table}_archive"

            cutoff_date = datetime.now() - timedelta(days=days)

            # Create archive table if not exists
            self.db.execute_query(f"""
            CREATE TABLE IF NOT EXISTS {archive_table} AS 
            SELECT * FROM {table} WHERE 1=0
            """)

            # Archive old data
            self.db.execute_query(f"""
            INSERT INTO {archive_table}
            SELECT * FROM {table}
            WHERE created_at < %s
            """, (cutoff_date,))

            # Delete archived data from source
            self.db.execute_query(f"""
            DELETE FROM {table}
            WHERE created_at < %s
            """, (cutoff_date,))

            logger.info(f"Archived data from {table} to {archive_table}")
            return True

        except Exception as e:
            logger.error(f"Error archiving data: {str(e)}")
            return False

    def get_table_sizes(self) -> Dict[str, int]:
        """
        Get sizes of tracked tables

        Returns:
            Dictionary with table sizes
        """
        sizes = {}

        for table in self.default_retention_days.keys():
            try:
                result = self.db.execute_query(f"""
                SELECT COUNT(*) as row_count
                FROM {table}
                """)

                if result and not result.empty:
                    sizes[table] = result['row_count'].iloc[0]
                else:
                    sizes[table] = 0

            except Exception as e:
                logger.error(f"Error getting size for {table}: {str(e)}")
                sizes[table] = -1

        return sizes

    def get_retention_report(self) -> Dict:
        """
        Generate retention report

        Returns:
            Report dictionary
        """
        report = {
            'policies': self.default_retention_days.copy(),
            'current_sizes': self.get_table_sizes(),
            'oldest_records': {}
        }

        for table in self.default_retention_days.keys():
            try:
                # Find oldest record
                date_columns = {
                    'user_sessions': 'expires_at',
                    'chat_history': 'created_at',
                    'query_cache': 'expires_at'
                }

                date_col = date_columns.get(table, 'created_at')

                result = self.db.execute_query(f"""
                SELECT MIN({date_col}) as oldest
                FROM {table}
                """)

                if result and not result.empty and result['oldest'].iloc[0]:
                    report['oldest_records'][table] = result['oldest'].iloc[0]
                else:
                    report['oldest_records'][table] = None

            except Exception as e:
                logger.error(f"Error getting oldest record for {table}: {str(e)}")
                report['oldest_records'][table] = None

        return report