"""
Dashboard Runner
Entry point for running the Streamlit dashboard
"""

import os
import sys
import argparse
import streamlit as st

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.dashboard.streamlit_app import run_dashboard


def main():
    """Main entry point for dashboard runner"""
    parser = argparse.ArgumentParser(description='Run DealerPulse Dashboard')
    parser.add_argument('--env', default='development', help='Environment (development/production)')
    parser.add_argument('--port', type=int, default=8501, help='Port to run on')
    parser.add_argument('--host', default='localhost', help='Host to bind to')

    args = parser.parse_args()

    # Set environment
    os.environ['APP_ENV'] = args.env

    # Run dashboard
    run_dashboard()


if __name__ == "__main__":
    main()