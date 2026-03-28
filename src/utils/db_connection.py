"""
Database Connection Module
Handles connections to Redshift and Aurora PostgreSQL
"""

import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
from typing import Dict, List, Optional, Any
import logging
import os

logger = logging.getLogger(__name__)


class RedshiftConnection:
    """Redshift database connection"""

    def __init__(self, config: Dict):
        """
        Initialize Redshift connection

        Args:
            config: Redshift configuration dictionary
        """
        self.config = config
        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        """
        Establish connection to Redshift

        Returns:
            Success flag
        """
        try:
            self.connection = psycopg2.connect(
                host=self.config.get('host'),
                port=self.config.get('port', 5439),
                database=self.config.get('database'),
                user=self.config.get('user'),
                password=self.config.get('password'),
                sslmode='require'
            )
            self.cursor = self.connection.cursor()
            logger.info("Connected to Redshift")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to Redshift: {str(e)}")
            return False

    def disconnect(self):
        """Close Redshift connection"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info("Disconnected from Redshift")
        except Exception as e:
            logger.error(f"Error disconnecting from Redshift: {str(e)}")

    def execute_query(self, query: str, params: Any = None) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            Pandas DataFrame with results
        """
        try:
            if not self.connection:
                if not self.connect():
                    return pd.DataFrame()

            return pd.read_sql_query(query, self.connection, params=params)

        except Exception as e:
            logger.error(f"Error executing Redshift query: {str(e)}")
            return pd.DataFrame()

    def execute_statement(self, query: str, params: Any = None) -> bool:
        """
        Execute SQL statement (INSERT, UPDATE, DELETE)

        Args:
            query: SQL statement
            params: Query parameters

        Returns:
            Success flag
        """
        try:
            if not self.connection:
                if not self.connect():
                    return False

            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                self.connection.commit()

            return True

        except Exception as e:
            logger.error(f"Error executing Redshift statement: {str(e)}")
            self.connection.rollback()
            return False

    def execute_many(self, query: str, params_list: List) -> bool:
        """
        Execute many statements

        Args:
            query: SQL statement
            params_list: List of parameter tuples

        Returns:
            Success flag
        """
        try:
            if not self.connection:
                if not self.connect():
                    return False

            with self.connection.cursor() as cursor:
                cursor.executemany(query, params_list)
                self.connection.commit()

            return True

        except Exception as e:
            logger.error(f"Error executing Redshift many statements: {str(e)}")
            self.connection.rollback()
            return False


class AuroraConnection:
    """Aurora PostgreSQL database connection"""

    def __init__(self, config: Dict):
        """
        Initialize Aurora connection

        Args:
            config: Aurora configuration dictionary
        """
        self.config = config
        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        """
        Establish connection to Aurora

        Returns:
            Success flag
        """
        try:
            self.connection = psycopg2.connect(
                host=self.config.get('host'),
                port=self.config.get('port', 5432),
                database=self.config.get('database'),
                user=self.config.get('user'),
                password=self.config.get('password'),
                sslmode='require'
            )
            self.cursor = self.connection.cursor()
            logger.info("Connected to Aurora PostgreSQL")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to Aurora: {str(e)}")
            return False

    def disconnect(self):
        """Close Aurora connection"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.info("Disconnected from Aurora PostgreSQL")
        except Exception as e:
            logger.error(f"Error disconnecting from Aurora: {str(e)}")

    def execute_query(self, query: str, params: Any = None) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            Pandas DataFrame with results
        """
        try:
            if not self.connection:
                if not self.connect():
                    return pd.DataFrame()

            return pd.read_sql_query(query, self.connection, params=params)

        except Exception as e:
            logger.error(f"Error executing Aurora query: {str(e)}")
            return pd.DataFrame()

    def execute_statement(self, query: str, params: Any = None) -> bool:
        """
        Execute SQL statement (INSERT, UPDATE, DELETE)

        Args:
            query: SQL statement
            params: Query parameters

        Returns:
            Success flag
        """
        try:
            if not self.connection:
                if not self.connect():
                    return False

            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                self.connection.commit()

            return True

        except Exception as e:
            logger.error(f"Error executing Aurora statement: {str(e)}")
            self.connection.rollback()
            return False

    def execute_many(self, query: str, params_list: List) -> bool:
        """
        Execute many statements

        Args:
            query: SQL statement
            params_list: List of parameter tuples

        Returns:
            Success flag
        """
        try:
            if not self.connection:
                if not self.connect():
                    return False

            with self.connection.cursor() as cursor:
                cursor.executemany(query, params_list)
                self.connection.commit()

            return True

        except Exception as e:
            logger.error(f"Error executing Aurora many statements: {str(e)}")
            self.connection.rollback()
            return False


def get_db_connection() -> Any:
    """
    Get database connection based on configuration

    Returns:
        Database connection object
    """
    from .config import Config

    config = Config()
    db_type = config.get('database.type', 'redshift')

    if db_type == 'redshift':
        redshift_config = config.get_redshift_config()
        conn = RedshiftConnection(redshift_config)
        conn.connect()
        return conn
    elif db_type == 'aurora':
        aurora_config = config.get_aurora_config()
        conn = AuroraConnection(aurora_config)
        conn.connect()
        return conn
    else:
        raise ValueError(f"Unsupported database type: {db_type}")