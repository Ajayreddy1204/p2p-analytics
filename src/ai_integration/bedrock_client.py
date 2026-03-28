"""
AWS Bedrock Client
Handles interactions with AWS Bedrock for LLM capabilities
"""

import boto3
import json
import logging
from typing import Dict, List, Optional, Any
from botocore.exceptions import ClientError
import time

logger = logging.getLogger(__name__)


class BedrockClient:
    """Client for AWS Bedrock LLM services"""

    def __init__(self, aws_config: Dict):
        """
        Initialize Bedrock client

        Args:
            aws_config: AWS configuration dictionary
        """
        self.region = aws_config.get('region', 'us-east-1')
        self.model_id = aws_config.get('bedrock_model', 'anthropic.claude-3-sonnet-20240229-v1:0')
        self.max_tokens = aws_config.get('max_tokens', 4096)
        self.temperature = aws_config.get('temperature', 0.7)

        # Initialize boto3 clients
        self.bedrock_runtime = boto3.client(
            service_name='bedrock-runtime',
            region_name=self.region
        )

        self.bedrock = boto3.client(
            service_name='bedrock',
            region_name=self.region
        )

        logger.info(f"Initialized Bedrock client with model: {self.model_id}")

    def generate_text(self, prompt: str, system_prompt: str = None,
                      max_tokens: int = None, temperature: float = None) -> str:
        """
        Generate text using Bedrock LLM

        Args:
            prompt: User prompt
            system_prompt: System instructions
            max_tokens: Maximum tokens to generate
            temperature: Temperature for randomness

        Returns:
            Generated text
        """
        try:
            # Prepare request body
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens or self.max_tokens,
                "temperature": temperature or self.temperature,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }

            if system_prompt:
                request_body["system"] = system_prompt

            # Invoke model
            response = self.bedrock_runtime.invoke_model(
                modelId=self.model_id,
                body=json.dumps(request_body)
            )

            # Parse response
            response_body = json.loads(response['body'].read())

            if 'content' in response_body and len(response_body['content']) > 0:
                return response_body['content'][0].get('text', '')
            else:
                return ''

        except ClientError as e:
            logger.error(f"Bedrock API error: {str(e)}")
            return f"Error generating response: {str(e)}"
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return f"Error: {str(e)}"

    def generate_sql(self, question: str, schema_context: str) -> str:
        """
        Generate SQL query from natural language question

        Args:
            question: User question
            schema_context: Database schema information

        Returns:
            SQL query string
        """
        system_prompt = """You are a SQL expert for a dealer analytics database. 
        Generate valid SQL queries based on the user's question.
        Use the provided schema information.
        Always use table aliases and qualify column names.
        Return ONLY the SQL query, no explanations."""

        prompt = f"""
        Schema information:
        {schema_context}

        Question: {question}

        Generate SQL query:
        """

        sql = self.generate_text(prompt, system_prompt, max_tokens=1024, temperature=0.1)

        # Clean up SQL (remove markdown code blocks, etc.)
        sql = sql.strip()
        if sql.startswith('```sql'):
            sql = sql[6:]
        if sql.startswith('```'):
            sql = sql[3:]
        if sql.endswith('```'):
            sql = sql[:-3]

        return sql.strip()

    def generate_insights(self, data_summary: str, question: str) -> Dict[str, str]:
        """
        Generate descriptive, prescriptive, and predictive insights

        Args:
            data_summary: Summary of data results
            question: Original user question

        Returns:
            Dictionary with descriptive, prescriptive, predictive sections
        """
        system_prompt = """You are a dealer business analyst. 
        Generate insights in three sections:
        1. Descriptive: What the data shows (2-3 sentences with key numbers)
        2. Prescriptive: Recommendations (5-7 bullet points starting with •)
        3. Predictive: Expected impact in 12-24 months (1-2 sentences)

        Be specific, cite numbers and dealer names from the data.
        Use business language, not technical jargon."""

        prompt = f"""
        Question: {question}

        Data:
        {data_summary}

        Generate insights:
        """

        response = self.generate_text(prompt, system_prompt, max_tokens=2048)

        # Parse sections
        sections = {
            'descriptive': '',
            'prescriptive': '',
            'predictive': ''
        }

        current_section = None
        for line in response.split('\n'):
            line_lower = line.lower()
            if 'descriptive' in line_lower:
                current_section = 'descriptive'
                continue
            elif 'prescriptive' in line_lower:
                current_section = 'prescriptive'
                continue
            elif 'predictive' in line_lower:
                current_section = 'predictive'
                continue

            if current_section and line.strip():
                sections[current_section] += line + '\n'

        return sections

    def generate_recommendations(self, dealer_name: str, metrics: Dict) -> List[str]:
        """
        Generate specific recommendations for a dealer

        Args:
            dealer_name: Dealer name
            metrics: Dictionary of dealer metrics

        Returns:
            List of recommendation strings
        """
        system_prompt = """You are a dealer performance consultant.
        Provide 3-5 specific, actionable recommendations for this dealer.
        Focus on the metrics that need improvement.
        Be concrete and include expected outcomes."""

        metrics_str = '\n'.join([f"- {k}: {v}" for k, v in metrics.items() if v is not None])

        prompt = f"""
        Dealer: {dealer_name}

        Current metrics:
        {metrics_str}

        Provide recommendations:
        """

        response = self.generate_text(prompt, system_prompt, max_tokens=1024)

        # Parse bullet points
        recommendations = []
        for line in response.split('\n'):
            line = line.strip()
            if line.startswith('•') or line.startswith('-') or line.startswith('*'):
                recommendations.append(line.lstrip('•-* ').strip())
            elif line and line[0].isdigit() and '.' in line[:3]:
                recommendations.append(line.split('.', 1)[1].strip())

        return recommendations

    def list_models(self) -> List[Dict]:
        """
        List available Bedrock models

        Returns:
            List of model information
        """
        try:
            response = self.bedrock.list_foundation_models()
            models = []

            for model in response.get('modelSummaries', []):
                models.append({
                    'model_id': model.get('modelId'),
                    'model_name': model.get('modelName'),
                    'provider': model.get('providerName'),
                    'input_modalities': model.get('inputModalities', []),
                    'output_modalities': model.get('outputModalities', [])
                })

            return models

        except Exception as e:
            logger.error(f"Error listing models: {str(e)}")
            return []

    def get_model_info(self) -> Dict:
        """
        Get information about current model

        Returns:
            Model information dictionary
        """
        try:
            response = self.bedrock.get_foundation_model(
                modelIdentifier=self.model_id
            )

            model = response.get('modelDetails', {})
            return {
                'model_id': model.get('modelId'),
                'model_name': model.get('modelName'),
                'provider': model.get('providerName'),
                'input_modalities': model.get('inputModalities', []),
                'output_modalities': model.get('outputModalities', []),
                'customizations_supported': model.get('customizationsSupported', [])
            }

        except Exception as e:
            logger.error(f"Error getting model info: {str(e)}")
            return {}

    def invoke_with_retry(self, prompt: str, max_retries: int = 3,
                          retry_delay: float = 1.0) -> str:
        """
        Invoke Bedrock with retry logic

        Args:
            prompt: User prompt
            max_retries: Maximum number of retries
            retry_delay: Delay between retries in seconds

        Returns:
            Generated text
        """
        for attempt in range(max_retries):
            try:
                return self.generate_text(prompt)
            except ClientError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Bedrock attempt {attempt + 1} failed: {str(e)}")
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                else:
                    raise
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(retry_delay)

        return ""