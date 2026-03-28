"""
Visualizations Module
Handles chart generation and data visualization for the dashboard
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import altair as alt
from typing import Dict, List, Optional, Tuple, Any
import logging

logger = logging.getLogger(__name__)


class Visualizer:
    """Generate charts and visualizations for the dashboard"""

    def __init__(self, theme: str = "light"):
        """
        Initialize visualizer with theme settings

        Args:
            theme: Color theme ('light' or 'dark')
        """
        self.theme = theme
        self._set_theme_colors()

    def _set_theme_colors(self):
        """Set color scheme based on theme"""
        if self.theme == 'dark':
            self.colors = {
                'primary': '#3b82f6',
                'secondary': '#8b5cf6',
                'success': '#10b981',
                'warning': '#f59e0b',
                'danger': '#ef4444',
                'grid': '#374151',
                'background': '#1f2937',
                'text': '#f3f4f6'
            }
        else:
            self.colors = {
                'primary': '#2563eb',
                'secondary': '#8b5cf6',
                'success': '#10b981',
                'warning': '#f59e0b',
                'danger': '#ef4444',
                'grid': '#e5e7eb',
                'background': '#ffffff',
                'text': '#111827'
            }

    def _apply_theme(self, fig: go.Figure) -> go.Figure:
        """Apply theme styling to a plotly figure"""
        fig.update_layout(
            template='plotly_white',
            plot_bgcolor=self.colors['background'],
            paper_bgcolor=self.colors['background'],
            font=dict(color=self.colors['text']),
            xaxis=dict(
                showgrid=True,
                gridcolor=self.colors['grid'],
                showline=True,
                linecolor=self.colors['grid']
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor=self.colors['grid'],
                showline=True,
                linecolor=self.colors['grid']
            )
        )
        return fig

    def revenue_trend_chart(self, df: pd.DataFrame, title: str = "Revenue Trend") -> go.Figure:
        """
        Create revenue trend line chart

        Args:
            df: DataFrame with 'period' and 'total_revenue' columns
            title: Chart title

        Returns:
            Plotly figure object
        """
        if df is None or df.empty:
            return go.Figure()

        fig = go.Figure()

        # Add revenue line
        fig.add_trace(go.Scatter(
            x=df['period'],
            y=df['total_revenue'],
            mode='lines+markers',
            name='Revenue',
            line=dict(color=self.colors['primary'], width=2),
            marker=dict(size=6, color=self.colors['primary']),
            hovertemplate='Period: %{x}<br>Revenue: $%{y:,.0f}<extra></extra>'
        ))

        # Add average line
        avg_revenue = df['total_revenue'].mean()
        fig.add_hline(
            y=avg_revenue,
            line_dash="dash",
            line_color=self.colors['warning'],
            annotation_text=f"Avg: ${avg_revenue:,.0f}",
            annotation_position="top right"
        )

        fig.update_layout(
            title=title,
            xaxis_title="Period",
            yaxis_title="Revenue ($)",
            hovermode='x unified'
        )

        return self._apply_theme(fig)

    def profit_margin_bar_chart(self, df: pd.DataFrame, title: str = "Gross Profit Margin by Dealer") -> go.Figure:
        """
        Create profit margin bar chart

        Args:
            df: DataFrame with 'dealer_name' and 'gross_profit_margin_pct' columns
            title: Chart title

        Returns:
            Plotly figure object
        """
        if df is None or df.empty:
            return go.Figure()

        fig = go.Figure()

        # Sort by margin
        df = df.sort_values('gross_profit_margin_pct', ascending=True)

        # Color based on performance
        colors = [
            self.colors['success'] if m >= 30
            else self.colors['warning'] if m >= 20
            else self.colors['danger']
            for m in df['gross_profit_margin_pct']
        ]

        fig.add_trace(go.Bar(
            x=df['gross_profit_margin_pct'],
            y=df['dealer_name'],
            orientation='h',
            marker=dict(color=colors, opacity=0.8),
            hovertemplate='Dealer: %{y}<br>Margin: %{x:.1f}%<extra></extra>'
        ))

        # Add target line
        fig.add_vline(
            x=30,
            line_dash="dash",
            line_color=self.colors['success'],
            annotation_text="Target: 30%",
            annotation_position="top right"
        )

        fig.update_layout(
            title=title,
            xaxis_title="Gross Profit Margin (%)",
            yaxis_title="Dealer",
            height=400,
            margin=dict(l=100, r=20, t=40, b=20)
        )

        return self._apply_theme(fig)

    def sales_mix_pie_chart(self, df: pd.DataFrame, title: str = "Sales Mix by Product Category") -> go.Figure:
        """
        Create sales mix pie/donut chart

        Args:
            df: DataFrame with 'product_category' and 'total_revenue' columns
            title: Chart title

        Returns:
            Plotly figure object
        """
        if df is None or df.empty:
            return go.Figure()

        # Sort by revenue
        df = df.sort_values('total_revenue', ascending=False)

        fig = go.Figure(data=[go.Pie(
            labels=df['product_category'],
            values=df['total_revenue'],
            hole=0.4,
            marker=dict(colors=px.colors.qualitative.Set3),
            textinfo='label+percent',
            hovertemplate='Category: %{label}<br>Revenue: $%{value:,.0f}<br>Share: %{percent}<extra></extra>'
        )])

        fig.update_layout(
            title=title,
            height=400,
            showlegend=True,
            legend=dict(orientation='v', yanchor='middle', y=0.5, xanchor='left', x=1.05)
        )

        return self._apply_theme(fig)

    def ccc_bar_chart(self, df: pd.DataFrame, title: str = "Cash Conversion Cycle by Dealer") -> go.Figure:
        """
        Create cash conversion cycle bar chart

        Args:
            df: DataFrame with 'dealer_name' and 'ccc' columns
            title: Chart title

        Returns:
            Plotly figure object
        """
        if df is None or df.empty:
            return go.Figure()

        # Sort by CCC
        df = df.sort_values('ccc', ascending=False)

        # Color based on performance
        colors = [
            self.colors['success'] if c <= 30
            else self.colors['warning'] if c <= 45
            else self.colors['danger']
            for c in df['ccc']
        ]

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=df['dealer_name'],
            y=df['ccc'],
            marker=dict(color=colors, opacity=0.8),
            hovertemplate='Dealer: %{x}<br>CCC: %{y:.1f} days<extra></extra>'
        ))

        # Add target line
        fig.add_hline(
            y=45,
            line_dash="dash",
            line_color=self.colors['warning'],
            annotation_text="Target: 45 days",
            annotation_position="top right"
        )

        fig.update_layout(
            title=title,
            xaxis_title="Dealer",
            yaxis_title="Cash Conversion Cycle (Days)",
            xaxis_tickangle=-45,
            height=400
        )

        return self._apply_theme(fig)

    def lead_time_bar_chart(self, df: pd.DataFrame, title: str = "Order Lead Time by Dealer") -> go.Figure:
        """
        Create order lead time bar chart

        Args:
            df: DataFrame with 'dealer_name' and 'avg_lead_time' columns
            title: Chart title

        Returns:
            Plotly figure object
        """
        if df is None or df.empty:
            return go.Figure()

        # Sort by lead time
        df = df.sort_values('avg_lead_time', ascending=False)

        # Color based on performance
        colors = [
            self.colors['success'] if l <= 5
            else self.colors['warning'] if l <= 7
            else self.colors['danger']
            for l in df['avg_lead_time']
        ]

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=df['dealer_name'],
            y=df['avg_lead_time'],
            marker=dict(color=colors, opacity=0.8),
            hovertemplate='Dealer: %{x}<br>Lead Time: %{y:.1f} days<extra></extra>'
        ))

        # Add target line
        fig.add_hline(
            y=7,
            line_dash="dash",
            line_color=self.colors['warning'],
            annotation_text="SLA: 7 days",
            annotation_position="top right"
        )

        fig.update_layout(
            title=title,
            xaxis_title="Dealer",
            yaxis_title="Order Lead Time (Days)",
            xaxis_tickangle=-45,
            height=400
        )

        return self._apply_theme(fig)

    def health_score_gauge(self, score: float, title: str = "Health Score") -> go.Figure:
        """
        Create a gauge chart for health score

        Args:
            score: Health score value (0-100)
            title: Chart title

        Returns:
            Plotly figure object
        """
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=score,
            title={'text': title},
            domain={'x': [0, 1], 'y': [0, 1]},
            gauge={
                'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
                'bar': {'color': self.colors['primary']},
                'bgcolor': "white",
                'borderwidth': 2,
                'bordercolor': "gray",
                'steps': [
                    {'range': [0, 25], 'color': self.colors['danger']},
                    {'range': [25, 50], 'color': self.colors['warning']},
                    {'range': [50, 75], 'color': '#ffd966'},
                    {'range': [75, 100], 'color': self.colors['success']}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': score
                }
            }
        ))

        fig.update_layout(height=300)

        return self._apply_theme(fig)

    def comparison_bar_chart(self, df: pd.DataFrame, x_col: str, y_cols: List[str],
                             labels: List[str], title: str = "Comparison Chart") -> go.Figure:
        """
        Create grouped bar chart for comparison

        Args:
            df: DataFrame with data
            x_col: Column for x-axis
            y_cols: List of columns for y-axis
            labels: Labels for each y column
            title: Chart title

        Returns:
            Plotly figure object
        """
        if df is None or df.empty:
            return go.Figure()

        fig = go.Figure()

        for y_col, label in zip(y_cols, labels):
            fig.add_trace(go.Bar(
                x=df[x_col],
                y=df[y_col],
                name=label,
                marker=dict(opacity=0.8),
                hovertemplate=f'{label}: %{{y:,.0f}}<extra></extra>'
            ))

        fig.update_layout(
            title=title,
            xaxis_title=x_col.replace('_', ' ').title(),
            yaxis_title="Value",
            barmode='group',
            height=400,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='left', x=0)
        )

        return self._apply_theme(fig)

    def line_multi_metric_chart(self, df: pd.DataFrame, x_col: str, y_cols: List[str],
                                labels: List[str], title: str = "Metric Trends") -> go.Figure:
        """
        Create line chart with multiple metrics

        Args:
            df: DataFrame with data
            x_col: Column for x-axis
            y_cols: List of columns for y-axis
            labels: Labels for each y column
            title: Chart title

        Returns:
            Plotly figure object
        """
        if df is None or df.empty:
            return go.Figure()

        fig = go.Figure()

        for y_col, label in zip(y_cols, labels):
            fig.add_trace(go.Scatter(
                x=df[x_col],
                y=df[y_col],
                mode='lines+markers',
                name=label,
                line=dict(width=2),
                marker=dict(size=4),
                hovertemplate=f'{label}: %{{y:.1f}}<extra></extra>'
            ))

        fig.update_layout(
            title=title,
            xaxis_title=x_col.replace('_', ' ').title(),
            yaxis_title="Value",
            hovermode='x unified',
            height=400,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='left', x=0)
        )

        return self._apply_theme(fig)

    def heatmap(self, df: pd.DataFrame, title: str = "Correlation Heatmap") -> go.Figure:
        """
        Create correlation heatmap

        Args:
            df: DataFrame with numeric columns
            title: Chart title

        Returns:
            Plotly figure object
        """
        if df is None or df.empty:
            return go.Figure()

        # Calculate correlation matrix
        corr = df.select_dtypes(include=[np.number]).corr()

        fig = go.Figure(data=go.Heatmap(
            z=corr.values,
            x=corr.columns,
            y=corr.index,
            colorscale='RdBu',
            zmin=-1,
            zmax=1,
            text=corr.values.round(2),
            texttemplate='%{text}',
            textfont={"size": 10},
            hovertemplate='%{x} vs %{y}: %{z:.2f}<extra></extra>'
        ))

        fig.update_layout(
            title=title,
            height=500,
            width=600
        )

        return self._apply_theme(fig)

    def create_altair_chart(self, df: pd.DataFrame, x: str, y: str,
                            chart_type: str = "bar", title: str = "") -> alt.Chart:
        """
        Create Altair chart for simple visualizations

        Args:
            df: DataFrame with data
            x: Column for x-axis
            y: Column for y-axis
            chart_type: Type of chart ('bar', 'line', 'point')
            title: Chart title

        Returns:
            Altair chart object
        """
        if df is None or df.empty:
            return None

        if chart_type == "bar":
            chart = alt.Chart(df).mark_bar().encode(
                x=alt.X(x, sort='-y'),
                y=alt.Y(y),
                tooltip=[x, y]
            )
        elif chart_type == "line":
            chart = alt.Chart(df).mark_line(point=True).encode(
                x=x,
                y=y,
                tooltip=[x, y]
            )
        elif chart_type == "point":
            chart = alt.Chart(df).mark_point().encode(
                x=x,
                y=y,
                tooltip=[x, y]
            )
        else:
            chart = alt.Chart(df).mark_bar().encode(
                x=x,
                y=y,
                tooltip=[x, y]
            )

        if title:
            chart = chart.properties(title=title)

        chart = chart.interactive()

        return chart