"""
Cost Tracker for AI Services
Tracks usage and costs for AWS Bedrock and Amazon Q
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)


class CostTracker:
    """Track usage and estimated costs for AI services"""

    # Model pricing per 1,000 tokens (approximate)
    MODEL_PRICING = {
        'anthropic.claude-3-sonnet-20240229-v1:0': {
            'input': 0.003,
            'output': 0.015
        },
        'anthropic.claude-3-haiku-20240307-v1:0': {
            'input': 0.00025,
            'output': 0.00125
        },
        'meta.llama3-8b-instruct-v1:0': {
            'input': 0.0005,
            'output': 0.0005
        },
        'amazon.titan-text-express-v1': {
            'input': 0.0008,
            'output': 0.0008
        }
    }

    def __init__(self):
        """Initialize cost tracker"""
        self.usage_log = []
        self.session_usage = defaultdict(lambda: {
            'input_tokens': 0,
            'output_tokens': 0,
            'requests': 0,
            'estimated_cost': 0.0
        })

    def track_request(self, model_id: str, input_tokens: int, output_tokens: int,
                      purpose: str = 'general') -> Dict:
        """
        Track a single AI request

        Args:
            model_id: ID of the model used
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            purpose: Purpose of the request (e.g., 'sql_generation', 'insights')

        Returns:
            Cost information dictionary
        """
        pricing = self.MODEL_PRICING.get(model_id, {'input': 0.001, 'output': 0.001})

        input_cost = (input_tokens / 1000) * pricing['input']
        output_cost = (output_tokens / 1000) * pricing['output']
        total_cost = input_cost + output_cost

        record = {
            'timestamp': datetime.now(),
            'model_id': model_id,
            'purpose': purpose,
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'input_cost': input_cost,
            'output_cost': output_cost,
            'total_cost': total_cost
        }

        self.usage_log.append(record)

        # Update session totals
        session_id = datetime.now().strftime('%Y-%m-%d')
        self.session_usage[session_id]['input_tokens'] += input_tokens
        self.session_usage[session_id]['output_tokens'] += output_tokens
        self.session_usage[session_id]['requests'] += 1
        self.session_usage[session_id]['estimated_cost'] += total_cost

        return {
            'input_cost': input_cost,
            'output_cost': output_cost,
            'total_cost': total_cost,
            'total_tokens': input_tokens + output_tokens
        }

    def get_session_summary(self, days: int = 7) -> Dict:
        """
        Get summary for the last N days

        Args:
            days: Number of days to summarize

        Returns:
            Summary dictionary
        """
        cutoff = datetime.now() - timedelta(days=days)

        summary = {
            'total_requests': 0,
            'total_input_tokens': 0,
            'total_output_tokens': 0,
            'total_cost': 0.0,
            'by_purpose': defaultdict(lambda: {
                'requests': 0,
                'tokens': 0,
                'cost': 0.0
            })
        }

        for record in self.usage_log:
            if record['timestamp'] >= cutoff:
                summary['total_requests'] += 1
                summary['total_input_tokens'] += record['input_tokens']
                summary['total_output_tokens'] += record['output_tokens']
                summary['total_cost'] += record['total_cost']

                purpose = record['purpose']
                summary['by_purpose'][purpose]['requests'] += 1
                summary['by_purpose'][purpose]['tokens'] += record['input_tokens'] + record['output_tokens']
                summary['by_purpose'][purpose]['cost'] += record['total_cost']

        return dict(summary)

    def get_daily_summary(self) -> Dict:
        """
        Get daily usage summary

        Returns:
            Dictionary with daily summaries
        """
        daily = {}

        for date, usage in self.session_usage.items():
            daily[date] = dict(usage)

        return daily

    def estimate_cost(self, input_tokens: int, output_tokens: int, model_id: str = None) -> float:
        """
        Estimate cost for a request

        Args:
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            model_id: Model ID (uses default if not specified)

        Returns:
            Estimated cost
        """
        if model_id is None:
            model_id = 'anthropic.claude-3-sonnet-20240229-v1:0'

        pricing = self.MODEL_PRICING.get(model_id, {'input': 0.001, 'output': 0.001})

        input_cost = (input_tokens / 1000) * pricing['input']
        output_cost = (output_tokens / 1000) * pricing['output']

        return input_cost + output_cost

    def export_usage(self, filepath: str):
        """
        Export usage log to file

        Args:
            filepath: Path to export file
        """
        export_data = []

        for record in self.usage_log:
            export_data.append({
                'timestamp': record['timestamp'].isoformat(),
                'model_id': record['model_id'],
                'purpose': record['purpose'],
                'input_tokens': record['input_tokens'],
                'output_tokens': record['output_tokens'],
                'input_cost': record['input_cost'],
                'output_cost': record['output_cost'],
                'total_cost': record['total_cost']
            })

        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2)

        logger.info(f"Usage exported to {filepath}")

    def clear_logs(self):
        """Clear usage logs"""
        self.usage_log.clear()
        self.session_usage.clear()
        logger.info("Usage logs cleared")

    def get_metrics(self) -> Dict:
        """
        Get cost metrics for monitoring

        Returns:
            Dictionary with key metrics
        """
        today = datetime.now().strftime('%Y-%m-%d')
        this_month = datetime.now().strftime('%Y-%m')

        total_cost = sum(record['total_cost'] for record in self.usage_log)
        today_cost = sum(r['total_cost'] for r in self.usage_log
                         if r['timestamp'].strftime('%Y-%m-%d') == today)
        this_month_cost = sum(r['total_cost'] for r in self.usage_log
                              if r['timestamp'].strftime('%Y-%m') == this_month)

        return {
            'total_requests': len(self.usage_log),
            'total_cost': total_cost,
            'today_cost': today_cost,
            'this_month_cost': this_month_cost,
            'avg_cost_per_request': total_cost / len(self.usage_log) if self.usage_log else 0
        }