"""
Layout Components Module
Contains UI components for the dashboard layout
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import re
import html
import logging

logger = logging.getLogger(__name__)


class LayoutComponents:
    """Render dashboard layout components"""

    def __init__(self):
        """Initialize layout components"""
        self._load_styles()

    def _load_styles(self):
        """Load CSS styles for the dashboard"""
        st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

        * {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        }

        /* KPI Cards */
        .kpi-card {
            background: linear-gradient(135deg, #ffffff 0%, #f9fafb 100%);
            border-radius: 16px;
            padding: 20px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            border: 1px solid #e5e7eb;
            transition: transform 0.2s, box-shadow 0.2s;
        }

        .kpi-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
        }

        .kpi-title {
            font-size: 14px;
            font-weight: 600;
            color: #6b7280;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }

        .kpi-value {
            font-size: 32px;
            font-weight: 800;
            color: #111827;
            line-height: 1.2;
        }

        .kpi-delta {
            font-size: 14px;
            font-weight: 500;
            margin-top: 8px;
        }

        .delta-positive {
            color: #10b981;
        }

        .delta-negative {
            color: #ef4444;
        }

        /* Insight Box */
        .insight-box {
            background: linear-gradient(135deg, #f3e8ff 0%, #f5f0ff 100%);
            border-radius: 12px;
            padding: 20px;
            border-left: 4px solid #8b5cf6;
            margin-bottom: 20px;
        }

        .insight-title {
            font-size: 14px;
            font-weight: 600;
            color: #7c3aed;
            margin-bottom: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .insight-item {
            font-size: 14px;
            color: #374151;
            margin-bottom: 8px;
            line-height: 1.5;
        }

        /* Alert Cards */
        .alert-card {
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 12px;
            border-left: 4px solid;
        }

        .alert-high {
            background: #fef2f2;
            border-left-color: #ef4444;
        }

        .alert-medium {
            background: #fffbeb;
            border-left-color: #f59e0b;
        }

        .alert-low {
            background: #f0fdf4;
            border-left-color: #10b981;
        }

        .alert-title {
            font-size: 14px;
            font-weight: 700;
            margin-bottom: 4px;
        }

        .alert-detail {
            font-size: 12px;
            color: #6b7280;
        }

        /* Filter Pills */
        .filter-pill {
            background: white;
            border: 1px solid #e5e7eb;
            border-radius: 999px;
            padding: 8px 16px;
            font-size: 14px;
            font-weight: 500;
            color: #374151;
            cursor: pointer;
            transition: all 0.2s;
        }

        .filter-pill:hover {
            border-color: #8b5cf6;
            color: #8b5cf6;
        }

        .filter-pill-active {
            background: #8b5cf6;
            border-color: #8b5cf6;
            color: white;
        }

        /* Navigation Buttons */
        .nav-button {
            background: transparent;
            border: none;
            padding: 8px 16px;
            font-size: 14px;
            font-weight: 500;
            color: #6b7280;
            cursor: pointer;
            transition: all 0.2s;
        }

        .nav-button:hover {
            color: #8b5cf6;
        }

        .nav-button-active {
            color: #8b5cf6;
            font-weight: 600;
            border-bottom: 2px solid #8b5cf6;
        }

        /* Section Containers */
        .section-container {
            background: white;
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 24px;
            border: 1px solid #e5e7eb;
            box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
        }

        .section-title {
            font-size: 18px;
            font-weight: 700;
            color: #111827;
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }

        /* Footer */
        .footer {
            text-align: center;
            padding: 24px;
            color: #9ca3af;
            font-size: 12px;
            border-top: 1px solid #e5e7eb;
            margin-top: 32px;
        }
        </style>
        """, unsafe_allow_html=True)

    def render_header(self):
        """Render dashboard header with navigation"""
        # Header container
        col1, col2, col3 = st.columns([1.5, 3, 1.5])

        with col1:
            st.markdown("""
            <div style="display: flex; align-items: center; gap: 12px;">
                <div style="font-size: 28px;">📊</div>
                <div>
                    <div style="font-size: 20px; font-weight: 800; color: #111827;">DealerPulse</div>
                    <div style="font-size: 12px; color: #6b7280;">P2P Analytics Dashboard</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            # Navigation tabs
            nav_items = ['Dashboard', 'Genie', 'Dealer Life Cycle', 'AI Agents']
            current_page = st.session_state.get('current_page', 'Dashboard')

            cols = st.columns(len(nav_items))
            for idx, item in enumerate(nav_items):
                with cols[idx]:
                    if st.button(
                            item,
                            key=f"nav_{item}",
                            use_container_width=True,
                            type="secondary" if current_page != item else "primary"
                    ):
                        st.session_state.current_page = item
                        st.rerun()

        with col3:
            # Right side content (user, settings, etc.)
            st.markdown("""
            <div style="display: flex; justify-content: flex-end; align-items: center; gap: 16px;">
                <span style="font-size: 14px; color: #6b7280;">Welcome, Dealer Manager</span>
                <span style="font-size: 20px;">👤</span>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<div style='height: 20px'></div>", unsafe_allow_html=True)

    def render_filters(self, db, data_loader) -> Dict:
        """
        Render filter controls and return filter dictionary

        Args:
            db: Database connection
            data_loader: DataLoader instance

        Returns:
            Dictionary of filter values
        """
        st.markdown("""
        <style>
        div[data-testid="stSelectbox"] > div > div > div {
            border-radius: 999px !important;
            padding: 8px 16px !important;
            min-height: 44px !important;
        }
        div[data-testid="stDateInput"] > div > div > div {
            border-radius: 999px !important;
            padding: 8px 16px !important;
            min-height: 44px !important;
        }
        div[role="radiogroup"] {
            display: flex;
            gap: 8px;
        }
        div[role="radiogroup"] label {
            border-radius: 999px !important;
            padding: 8px 24px !important;
            background: white;
            border: 1px solid #e5e7eb;
            font-weight: 500;
        }
        div[role="radiogroup"] label[data-checked="true"] {
            background: #8b5cf6 !important;
            border-color: #8b5cf6 !important;
            color: white !important;
        }
        </style>
        """, unsafe_allow_html=True)

        # Get dealers and products
        dealers = data_loader.fetch_dealers()
        products = data_loader.fetch_products()

        # Filter columns
        col1, col2, col3, col4 = st.columns([2, 2, 2, 1.5])

        with col1:
            # Date range
            date_range = st.selectbox(
                "Date Range",
                ["Last 7 Days", "Last 30 Days", "Last 90 Days", "Last 6 Months", "Year to Date", "All Dates"],
                index=1,
                key="date_range"
            )

        with col2:
            # Dealer filter
            dealer_options = ["All Dealers"] + dealers
            selected_dealer = st.selectbox(
                "Dealer",
                dealer_options,
                index=0,
                key="dealer_filter"
            )

        with col3:
            # Product filter
            product_options = ["All Products"] + products
            selected_product = st.selectbox(
                "Product",
                product_options,
                index=0,
                key="product_filter"
            )

        with col4:
            # Time period comparison
            time_period = st.selectbox(
                "Compare",
                ["Current Period", "Previous Period", "YoY Comparison"],
                index=0,
                key="time_period"
            )

        # Compute date range based on selection
        today = datetime.now().date()

        if date_range == "Last 7 Days":
            from_date = today - timedelta(days=7)
            to_date = today
        elif date_range == "Last 30 Days":
            from_date = today - timedelta(days=30)
            to_date = today
        elif date_range == "Last 90 Days":
            from_date = today - timedelta(days=90)
            to_date = today
        elif date_range == "Last 6 Months":
            from_date = today - timedelta(days=180)
            to_date = today
        elif date_range == "Year to Date":
            from_date = datetime(today.year, 1, 1).date()
            to_date = today
        else:  # All Dates
            from_date = datetime(2020, 1, 1).date()
            to_date = today

        filters = {
            'from_date': from_date,
            'to_date': to_date,
            'dealer': selected_dealer,
            'product': selected_product if selected_product != "All Products" else None,
            'time_period': time_period,
            'date_range': date_range
        }

        st.session_state.filters = filters

        return filters

    def render_insights(self, db, filters: Dict):
        """
        Render strategic insights section

        Args:
            db: Database connection
            filters: Filter dictionary
        """
        if not st.session_state.get('show_insights', True):
            return

        with st.container():
            st.markdown("""
            <div class="insight-box">
                <div class="insight-title">📊 Strategic Insights</div>
            """, unsafe_allow_html=True)

            # Fetch insights from database or generate dynamically
            insights = self._generate_insights(db, filters)

            for insight in insights:
                st.markdown(f'<div class="insight-item">• {insight}</div>', unsafe_allow_html=True)

            st.markdown("</div>", unsafe_allow_html=True)

        # Hide button
        if st.button("Hide Insights", key="hide_insights", type="secondary"):
            st.session_state.show_insights = False
            st.rerun()

    def _generate_insights(self, db, filters: Dict) -> List[str]:
        """
        Generate insights based on current data

        Args:
            db: Database connection
            filters: Filter dictionary

        Returns:
            List of insight strings
        """
        insights = []

        try:
            # Get at-risk dealers count
            query = """
            SELECT COUNT(*) as at_risk_count
            FROM (
                SELECT 
                    dealer_name,
                    AVG(gross_profit_margin_pct) as avg_margin,
                    AVG(stock_availability_pct) as avg_stock
                FROM dealer_information_mart.vw_gross_profit_margin g
                JOIN dealer_information_mart.vw_stock_availability_dealer s 
                    ON g.dealer_name = s.dealer_name
                GROUP BY dealer_name
                HAVING avg_margin < 20 OR avg_stock < 70
            ) at_risk
            """

            result = db.execute_query(query)

            if result and not result.empty:
                at_risk_count = result['at_risk_count'].iloc[0]
                if at_risk_count > 0:
                    insights.append(
                        f"{at_risk_count} dealers flagged as AT-Risk based on margin or stock availability.")

            # Get revenue growth trends
            growth_query = """
            SELECT 
                AVG(revenue_growth_mom_percent) as avg_growth,
                COUNT(CASE WHEN revenue_growth_mom_percent < 0 THEN 1 END) as declining_count
            FROM dealer_information_mart.vw_dealer_revenue_growth
            WHERE period_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')
            """

            growth_result = db.execute_query(growth_query)

            if growth_result and not growth_result.empty:
                avg_growth = growth_result['avg_growth'].iloc[0]
                declining = growth_result['declining_count'].iloc[0]

                if avg_growth > 5:
                    insights.append(f"Revenue growth is strong at {avg_growth:.1f}% over the last 3 months.")
                elif avg_growth < 0:
                    insights.append(
                        f"Revenue is declining at {abs(avg_growth):.1f}% - investigate underperforming dealers.")

                if declining > 0:
                    insights.append(f"{declining} dealers showing negative revenue growth. Review their performance.")

            # Get CCC trend
            ccc_query = """
            SELECT 
                AVG(ccc) as avg_ccc,
                COUNT(CASE WHEN ccc > 45 THEN 1 END) as high_ccc_count
            FROM dealer_information_mart.vw_cash_conversion_cycle
            WHERE period_month >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '3 months')
            """

            ccc_result = db.execute_query(ccc_query)

            if ccc_result and not ccc_result.empty:
                avg_ccc = ccc_result['avg_ccc'].iloc[0]
                high_ccc = ccc_result['high_ccc_count'].iloc[0]

                if avg_ccc > 45:
                    insights.append(
                        f"Cash conversion cycle is high at {avg_ccc:.0f} days - focus on working capital optimization.")

                if high_ccc > 0:
                    insights.append(f"{high_ccc} dealers have CCC exceeding 45 days. Review their DSO and DIO metrics.")

        except Exception as e:
            logger.error(f"Error generating insights: {str(e)}")
            insights.append("Unable to generate insights at this time.")

        if not insights:
            insights.append("All metrics are within normal ranges. Good performance across the network.")

        return insights

    def render_kpi_metrics(self, kpi_calculator, filters: Dict):
        """
        Render KPI metric cards

        Args:
            kpi_calculator: KPICalculator instance
            filters: Filter dictionary
        """
        st.markdown('<div class="section-container">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Key Performance Indicators</div>', unsafe_allow_html=True)

        # Calculate KPIs
        kpis = {
            'CCC': kpi_calculator.calculate_cash_conversion_cycle(filters),
            'Repair TAT': kpi_calculator.calculate_repair_turnaround_time(filters),
            'Revenue Growth': kpi_calculator.calculate_revenue_growth(filters),
            'Gross Margin': kpi_calculator.calculate_gross_profit_margin(filters),
            'Stock Availability': kpi_calculator.calculate_stock_availability(filters),
            'Backorder': kpi_calculator.calculate_backorder_incidence(filters),
            'Order Lead Time': kpi_calculator.calculate_order_lead_time(filters),
            'Sales Volume': kpi_calculator.calculate_sales_volume(filters)
        }

        # Format functions
        def format_value(key, value):
            if value is None:
                return "N/A"

            if key in ['CCC', 'Order Lead Time']:
                return f"{value:.0f} days"
            elif key == 'Repair TAT':
                return f"{value:.0f} hrs"
            elif key in ['Revenue Growth', 'Gross Margin', 'Stock Availability', 'Backorder']:
                return f"{value:.1f}%"
            elif key == 'Sales Volume':
                if value >= 1_000_000:
                    return f"{value / 1_000_000:.1f}M"
                elif value >= 1_000:
                    return f"{value / 1_000:.1f}K"
                else:
                    return f"{value:,}"
            return str(value)

        def get_delta(key, value):
            # Calculate previous period values for delta
            if filters.get('time_period') != 'Current Period':
                prev_filters = filters.copy()

                if filters.get('time_period') == 'Previous Period':
                    # Calculate previous period date range
                    if filters.get('from_date') and filters.get('to_date'):
                        period_days = (filters['to_date'] - filters['from_date']).days
                        prev_from = filters['from_date'] - timedelta(days=period_days)
                        prev_to = filters['from_date'] - timedelta(days=1)
                        prev_filters['from_date'] = prev_from
                        prev_filters['to_date'] = prev_to
                elif filters.get('time_period') == 'YoY Comparison':
                    # Shift by 1 year
                    if filters.get('from_date') and filters.get('to_date'):
                        prev_filters['from_date'] = filters['from_date'] - timedelta(days=365)
                        prev_filters['to_date'] = filters['to_date'] - timedelta(days=365)

                # Calculate previous value
                if key == 'CCC':
                    prev_value = kpi_calculator.calculate_cash_conversion_cycle(prev_filters)
                elif key == 'Repair TAT':
                    prev_value = kpi_calculator.calculate_repair_turnaround_time(prev_filters)
                elif key == 'Revenue Growth':
                    prev_value = kpi_calculator.calculate_revenue_growth(prev_filters)
                elif key == 'Gross Margin':
                    prev_value = kpi_calculator.calculate_gross_profit_margin(prev_filters)
                elif key == 'Stock Availability':
                    prev_value = kpi_calculator.calculate_stock_availability(prev_filters)
                elif key == 'Backorder':
                    prev_value = kpi_calculator.calculate_backorder_incidence(prev_filters)
                elif key == 'Order Lead Time':
                    prev_value = kpi_calculator.calculate_order_lead_time(prev_filters)
                elif key == 'Sales Volume':
                    prev_value = kpi_calculator.calculate_sales_volume(prev_filters)
                else:
                    prev_value = None

                if value is not None and prev_value is not None:
                    delta = value - prev_value

                    # Determine if higher is better
                    higher_better = key in ['Revenue Growth', 'Gross Margin', 'Stock Availability', 'Sales Volume']

                    if higher_better:
                        is_positive = delta > 0
                    else:
                        is_positive = delta < 0

                    if abs(delta) >= 1:
                        if key in ['Revenue Growth', 'Gross Margin', 'Stock Availability', 'Backorder']:
                            delta_str = f"{delta:+.1f}%"
                        elif key in ['CCC', 'Order Lead Time', 'Repair TAT']:
                            delta_str = f"{delta:+.0f}"
                        else:
                            delta_str = f"{delta:+.0f}"
                    else:
                        delta_str = "0"

                    return {
                        'text': delta_str,
                        'class': 'delta-positive' if is_positive else 'delta-negative'
                    }

            return None

        # Create 2 rows of 4 columns
        kpi_items = list(kpis.items())

        for row in range(2):
            cols = st.columns(4)
            for col_idx in range(4):
                idx = row * 4 + col_idx
                if idx < len(kpi_items):
                    key, value = kpi_items[idx]
                    formatted_value = format_value(key, value)
                    delta = get_delta(key, value)

                    with cols[col_idx]:
                        st.markdown(f"""
                        <div class="kpi-card">
                            <div class="kpi-title">{key}</div>
                            <div class="kpi-value">{formatted_value}</div>
                            {f'<div class="kpi-delta {delta["class"]}">{delta["text"]} vs previous period</div>' if delta else '<div class="kpi-delta"> </div>'}
                        </div>
                        """, unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

    def render_attention_and_priority(self, db, kpi_calculator, filters: Dict):
        """
        Render attention and priority interventions

        Args:
            db: Database connection
            kpi_calculator: KPICalculator instance
            filters: Filter dictionary
        """
        st.markdown('<div class="section-container">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">⚠️ Priority Interventions</div>', unsafe_allow_html=True)

        # Generate alerts for all dealers
        alerts = self._generate_alerts(db, kpi_calculator, filters)

        if alerts:
            for alert in alerts:
                severity = alert.get('severity', 'medium')
                title = alert.get('title', '')
                detail = alert.get('detail', '')

                st.markdown(f"""
                <div class="alert-card alert-{severity}">
                    <div class="alert-title">{title}</div>
                    <div class="alert-detail">{detail}</div>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="text-align: center; padding: 40px; color: #9ca3af;">
                ✅ No critical alerts at this time. All dealers are performing within thresholds.
            </div>
            """, unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

    def _generate_alerts(self, db, kpi_calculator, filters: Dict) -> List[Dict]:
        """
        Generate alerts based on KPI thresholds

        Args:
            db: Database connection
            kpi_calculator: KPICalculator instance
            filters: Filter dictionary

        Returns:
            List of alert dictionaries
        """
        alerts = []

        try:
            # Get all dealers
            dealers_query = """
            SELECT DISTINCT dealer_name
            FROM dealer_information_mart.vw_dealer_location
            WHERE dealer_name IS NOT NULL
            ORDER BY dealer_name
            """

            dealers_df = db.execute_query(dealers_query)

            if dealers_df is None or dealers_df.empty:
                return alerts

            for _, row in dealers_df.iterrows():
                dealer_name = row['dealer_name']

                # Create dealer-specific filters
                dealer_filters = filters.copy() if filters else {}
                dealer_filters['dealer'] = dealer_name

                # Get KPIs for this dealer
                gross_margin = kpi_calculator.calculate_gross_profit_margin(dealer_filters)
                stock_avail = kpi_calculator.calculate_stock_availability(dealer_filters)
                repair_tat = kpi_calculator.calculate_repair_turnaround_time(dealer_filters)
                ccc = kpi_calculator.calculate_cash_conversion_cycle(dealer_filters)
                lead_time = kpi_calculator.calculate_order_lead_time(dealer_filters)
                backorder = kpi_calculator.calculate_backorder_incidence(dealer_filters)

                # Check thresholds
                if gross_margin is not None and gross_margin < 15:
                    alerts.append({
                        'severity': 'high' if gross_margin < 10 else 'medium',
                        'title': f'{dealer_name}: Low Gross Margin',
                        'detail': f'Gross margin at {gross_margin:.1f}% - below target of 15%. Review pricing and cost structure.'
                    })

                if stock_avail is not None and stock_avail < 70:
                    alerts.append({
                        'severity': 'high' if stock_avail < 50 else 'medium',
                        'title': f'{dealer_name}: Low Stock Availability',
                        'detail': f'Stock availability at {stock_avail:.1f}% - below target of 70%. Risk of stockouts.'
                    })

                if repair_tat is not None and repair_tat > 48:
                    alerts.append({
                        'severity': 'high' if repair_tat > 72 else 'medium',
                        'title': f'{dealer_name}: High Service Turnaround Time',
                        'detail': f'Repair TAT at {repair_tat:.0f} hours - exceeds SLA of 48 hours.'
                    })

                if ccc is not None and ccc > 60:
                    alerts.append({
                        'severity': 'high' if ccc > 75 else 'medium',
                        'title': f'{dealer_name}: Long Cash Conversion Cycle',
                        'detail': f'CCC at {ccc:.0f} days - exceeds target of 60 days. Review DSO and DIO.'
                    })

                if lead_time is not None and lead_time > 10:
                    alerts.append({
                        'severity': 'high' if lead_time > 14 else 'medium',
                        'title': f'{dealer_name}: Long Order Lead Time',
                        'detail': f'Order lead time at {lead_time:.0f} days - exceeds target of 7 days.'
                    })

                if backorder is not None and backorder > 15:
                    alerts.append({
                        'severity': 'high' if backorder > 25 else 'medium',
                        'title': f'{dealer_name}: High Backorder Rate',
                        'detail': f'Backorder incidence at {backorder:.1f}% - exceeds target of 15%.'
                    })

        except Exception as e:
            logger.error(f"Error generating alerts: {str(e)}")

        return alerts

    def render_visualizations(self, data_loader, filters: Dict):
        """
        Render charts and visualizations

        Args:
            data_loader: DataLoader instance
            filters: Filter dictionary
        """
        st.markdown('<div class="section-container">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">📈 Performance Analytics</div>', unsafe_allow_html=True)

        # Create two columns for charts
        col1, col2 = st.columns(2)

        with col1:
            # Revenue trend chart
            revenue_df = data_loader.fetch_revenue_trend(filters)
            if not revenue_df.empty:
                from .visualizations import Visualizer
                visualizer = Visualizer()
                fig = visualizer.revenue_trend_chart(revenue_df)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No revenue data available for the selected filters.")

        with col2:
            # Profit margin chart
            margin_df = data_loader.fetch_profit_margin_by_dealer(filters)
            if not margin_df.empty:
                from .visualizations import Visualizer
                visualizer = Visualizer()
                fig = visualizer.profit_margin_bar_chart(margin_df)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No margin data available for the selected filters.")

        # Second row of charts
        col3, col4 = st.columns(2)

        with col3:
            # Sales mix chart
            sales_df = data_loader.fetch_sales_by_product_category(filters)
            if not sales_df.empty:
                from .visualizations import Visualizer
                visualizer = Visualizer()
                fig = visualizer.sales_mix_pie_chart(sales_df)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No sales data available for the selected filters.")

        with col4:
            # CCC chart
            ccc_df = data_loader.fetch_cash_conversion_cycle_trend(filters)
            if not ccc_df.empty:
                from .visualizations import Visualizer
                visualizer = Visualizer()
                fig = visualizer.ccc_bar_chart(ccc_df)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No CCC data available for the selected filters.")

        st.markdown('</div>', unsafe_allow_html=True)

    def render_footer(self):
        """Render footer section"""
        st.markdown("""
        <div class="footer">
            <small>© 2024 DealerPulse. All rights reserved. | Data updated in real-time from Redshift</small>
        </div>
        """, unsafe_allow_html=True)