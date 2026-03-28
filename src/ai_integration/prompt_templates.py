"""
Prompt Templates for AI Integration
Contains reusable prompt templates for various AI tasks
"""


class PromptTemplates:
    """Collection of prompt templates for different AI tasks"""

    @staticmethod
    def sql_generation(question: str, schema: str) -> str:
        """Template for SQL generation"""
        return f"""You are a SQL expert for a dealer analytics database.
Generate a valid SQL query to answer the user's question.

Database Schema:
{schema}

User Question: {question}

Important Rules:
1. Use fully qualified table names: dealer_information_mart.view_name
2. Always use table aliases (e.g., FROM vw_sales_volume AS sv)
3. Always qualify column names with aliases (e.g., sv.dealer_name)
4. Use appropriate aggregations (SUM, AVG, COUNT) when needed
5. Add LIMIT 1000 for exploratory queries
6. Only return the SQL query, no explanations

SQL Query:"""

    @staticmethod
    def insight_generation(question: str, data_summary: str) -> str:
        """Template for generating insights"""
        return f"""You are a dealer business analyst. Based on the data below, provide insights in three sections.

User Question: {question}

Data Summary:
{data_summary}

Format your response exactly as:
**Descriptive**
[What the data shows - 2-3 sentences with key numbers]

**Prescriptive**
[Recommendations - 5-7 bullet points starting with •]

**Predictive**
[Expected impact in 12-24 months - 1-2 sentences]

Insights:"""

    @staticmethod
    def dealer_recommendations(dealer_name: str, metrics: dict) -> str:
        """Template for dealer-specific recommendations"""
        metrics_str = '\n'.join([f"- {k}: {v}" for k, v in metrics.items() if v is not None])

        return f"""You are a dealer performance consultant. Provide specific recommendations for this dealer.

Dealer: {dealer_name}

Current Metrics:
{metrics_str}

Provide 3-5 actionable recommendations:
1. Focus on metrics that need improvement
2. Be specific about what actions to take
3. Include expected outcomes

Recommendations:"""

    @staticmethod
    def route_question(question: str) -> str:
        """Template for routing user questions to appropriate handlers"""
        return f"""Classify this user question into one of these categories:

Categories:
- KPI_QUERY: Questions about specific metrics (margin, revenue, growth, etc.)
- COMPARISON: Comparing dealers, regions, or time periods
- TREND: Questions about trends over time
- FORECAST: Predictive questions
- INVENTORY: Questions about stock, backorders, availability
- SERVICE: Questions about repair, turnaround, efficiency
- GENERAL: Other business questions

Question: {question}

Category:"""

    @staticmethod
    def extract_entities(question: str) -> str:
        """Template for extracting entities from user questions"""
        return f"""Extract entities from this user question:

Question: {question}

Entities to extract:
- dealer_name: Dealer name or ID
- region: Geographic region
- product: Product category
- time_period: Time range mentioned
- metric: Specific metric mentioned
- limit: Number limit (like "top 5")

Return as JSON:
{{"dealer_name": "...", "region": "...", "product": "...", "time_period": "...", "metric": "...", "limit": ...}}

Entities:"""

    @staticmethod
    def explain_results(sql: str, results_df_summary: str, question: str) -> str:
        """Template for explaining query results"""
        return f"""Explain these query results in simple business terms.

Original Question: {question}

SQL Query:
{sql}

Results Summary:
{results_df_summary}

Provide a concise explanation (2-3 sentences) that answers the question directly.
Focus on key numbers and insights.

Explanation:"""

    @staticmethod
    def generate_alert(metric_name: str, dealer_name: str, value: float, threshold: float) -> str:
        """Template for generating alert messages"""
        return f"""Generate a concise alert message for this KPI threshold breach.

Metric: {metric_name}
Dealer: {dealer_name}
Current Value: {value}
Threshold: {threshold}

Create a brief alert (1 sentence) with:
- The issue
- The specific value
- A suggested action

Alert:"""

    @staticmethod
    def summarize_conversation(messages: list) -> str:
        """Template for summarizing a conversation"""
        transcript = '\n'.join([f"{m['role']}: {m['content']}" for m in messages])

        return f"""Summarize this dealer analytics conversation.

Conversation:
{transcript}

Provide a summary with:
1. Key questions asked
2. Key insights discovered
3. Actions taken or recommended

Summary:"""

    @staticmethod
    def forecast_prediction(dealer_name: str, historical_data: dict, forecast_data: dict) -> str:
        """Template for generating forecast predictions"""
        hist_str = '\n'.join([f"- {k}: {v}" for k, v in historical_data.items()])
        fore_str = '\n'.join([f"- {k}: {v}" for k, v in forecast_data.items()])

        return f"""Based on the forecast data, generate a prediction for this dealer.

Dealer: {dealer_name}

Historical Performance:
{hist_str}

Forecast:
{fore_str}

Provide:
1. What the forecast means for revenue trajectory
2. One key business implication
3. One suggested action if trend is negative, or opportunity if positive

Prediction:"""