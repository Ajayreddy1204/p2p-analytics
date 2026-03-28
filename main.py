"""
Main Entry Point
Data processing and ETL pipeline entry point
"""

import os
import sys
import argparse
import logging
from datetime import datetime

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.config import Config
from src.utils.logger import setup_logging
from src.utils.db_connection import get_db_connection
from src.data_processing.kpi_calculator import KPICalculator
from src.data_processing.data_loader import DataLoader
from src.session_management.retention_manager import RetentionManager

logger = setup_logging()


def process_kpis(config: Config, db_connection):
    """
    Process and calculate KPIs

    Args:
        config: Configuration object
        db_connection: Database connection
    """
    logger.info("Starting KPI processing...")

    kpi_calculator = KPICalculator(db_connection)
    data_loader = DataLoader(db_connection)

    # Process all dealers
    dealers = data_loader.fetch_dealers()
    logger.info(f"Processing {len(dealers)} dealers")

    for dealer in dealers:
        logger.debug(f"Processing dealer: {dealer}")

        # Calculate dealer health score
        health_score = kpi_calculator.calculate_dealer_health_score(dealer)

        # Store in database (implement as needed)
        # store_health_score(health_score)

    logger.info("KPI processing completed")


def cleanup_data(config: Config, db_connection):
    """
    Clean up old data based on retention policies

    Args:
        config: Configuration object
        db_connection: Database connection
    """
    logger.info("Starting data cleanup...")

    retention_manager = RetentionManager(db_connection)

    # Clean up all tables
    results = retention_manager.cleanup_all()

    for table, count in results.items():
        logger.info(f"Cleaned up {count} records from {table}")

    logger.info("Data cleanup completed")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='DealerPulse Data Processing')
    parser.add_argument('--env', default='development', help='Environment (development/production)')
    parser.add_argument('--task', default='process', choices=['process', 'cleanup', 'all'],
                        help='Task to run')

    args = parser.parse_args()

    # Set environment
    os.environ['APP_ENV'] = args.env

    # Load configuration
    config = Config()

    # Get database connection
    db_connection = get_db_connection()

    try:
        if args.task in ['process', 'all']:
            process_kpis(config, db_connection)

        if args.task in ['cleanup', 'all']:
            cleanup_data(config, db_connection)

        logger.info("Main process completed successfully")

    except Exception as e:
        logger.error(f"Error in main process: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        db_connection.disconnect()


if __name__ == "__main__":
    main()