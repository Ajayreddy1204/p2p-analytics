"""
Streamlit Dashboard Application
Main application entry point for the P2P Analytics Dashboard
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import yaml
import os
import sys
from typing import Dict, Any, Optional
import logging

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import Config
from utils.logger import setup_logging
from utils.db_connection import get_db_connection
from data_processing.kpi_calculator import KPICalculator
from data_processing.data_loader import DataLoader
from data_processing.data_validator import DataValidator
from dashboard.visualizations import Visualizer
from dashboard.layout_components import LayoutComponents
from ai_integration.bedrock_client import BedrockClient
from session_management.session_db import SessionManager

# Initialize logging
logger = setup_logging()

# Page configuration
st.set_page_config(
    page_title="DealerPulse - P2P Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)


class Dashboard:
    """Main dashboard application class"""

    def __init__(self):
        """Initialize dashboard with configuration and connections"""
        self.config = Config()
        self.db = get_db_connection()
        self.kpi_calculator = KPICalculator(self.db)
        self.data_loader = DataLoader(self.db)
        self.validator = DataValidator()
        self.visualizer = Visualizer()
        self.layout = LayoutComponents()
        self.session_manager = SessionManager(self.db)

        # AI client (optional)
        self.bedrock_client = None
        if self.config.get('ai.enabled', False):
            self.bedrock_client = BedrockClient(self.config.get('aws'))

        # Initialize session state
        self._init_session_state()

    def _init_session_state(self):
        """Initialize Streamlit session state variables"""
        defaults = {
            'current_page': 'Dashboard',
            'show_insights': True,
            'genie_cache': {},
            'genie_messages': [],
            'selected_dealer': 'All Dealers',
            'selected_date_range': 'Last 30 Days',
            'selected_time_period': 'Current Period',
            'selected_product': 'All Products',
            'filters': {},
            'debug_mode': False
        }

        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value

    def render_header(self):
        """Render dashboard header with navigation"""
        self.layout.render_header()

    def render_filters(self) -> Dict:
        """Render filter controls and return filter dictionary"""
        return self.layout.render_filters(self.db, self.data_loader)

    def render_insights(self, filters: Dict):
        """Render strategic insights section"""
        self.layout.render_insights(self.db, filters)

    def render_kpi_metrics(self, filters: Dict):
        """Render KPI metric cards"""
        self.layout.render_kpi_metrics(self.kpi_calculator, filters)

    def render_attention_and_priority(self, filters: Dict):
        """Render attention and priority interventions"""
        self.layout.render_attention_and_priority(self.db, self.kpi_calculator, filters)

    def render_visualizations(self, filters: Dict):
        """Render charts and visualizations"""
        self.layout.render_visualizations(self.data_loader, filters)

    def render_genie_page(self):
        """Render Genie AI assistant page"""
        from .genie_page import render_genie_page
        render_genie_page(self.db, self.bedrock_client, self.data_loader)

    def render_dealer_life_cycle_page(self):
        """Render Dealer Life Cycle analytics page"""
        from .dealer_life_cycle import render_dealer_life_cycle
        render_dealer_life_cycle(self.db, self.data_loader, self.kpi_calculator)

    def render_agent_ai_page(self):
        """Render AI Agents page"""
        from .agent_ai import render_agent_ai
        render_agent_ai(self.db, self.bedrock_client)

    def render_dashboard_page(self):
        """Render main dashboard page"""
        # Render filters
        filters = self.render_filters()

        # Render insights
        self.render_insights(filters)

        # Render KPI metrics
        self.render_kpi_metrics(filters)

        # Render attention and priority
        self.render_attention_and_priority(filters)

        # Render visualizations
        self.render_visualizations(filters)

    def run(self):
        """Main application entry point"""
        try:
            # Render header (includes navigation)
            self.render_header()

            # Route to appropriate page
            current_page = st.session_state.current_page

            if current_page == 'Dashboard':
                self.render_dashboard_page()
            elif current_page == 'Genie':
                self.render_genie_page()
            elif current_page == 'Dealer Life Cycle':
                self.render_dealer_life_cycle_page()
            elif current_page == 'AI Agents':
                self.render_agent_ai_page()

            # Footer
            self.layout.render_footer()

        except Exception as e:
            logger.error(f"Dashboard error: {str(e)}", exc_info=True)
            st.error(f"An error occurred: {str(e)}")
            if st.session_state.debug_mode:
                st.exception(e)


def run_dashboard():
    """Entry point for running the dashboard"""
    dashboard = Dashboard()
    dashboard.run()


if __name__ == "__main__":
    run_dashboard()