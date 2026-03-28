"""
User Context Module
Manages user-specific context and preferences
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class UserContext:
    """Manage user-specific context and preferences"""

    def __init__(self, db_connection):
        """
        Initialize user context manager

        Args:
            db_connection: Database connection object
        """
        self.db = db_connection
        self._create_context_table()

    def _create_context_table(self):
        """Create user context table if it doesn't exist"""
        try:
            self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS user_context (
                user_id VARCHAR(255) PRIMARY KEY,
                preferences JSONB,
                recent_queries JSONB,
                favorite_dealers JSONB,
                notification_settings JSONB,
                last_active TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)

            logger.info("User context table created/verified")

        except Exception as e:
            logger.error(f"Error creating user context table: {str(e)}")

    def get_user_context(self, user_id: str) -> Dict:
        """
        Get user context

        Args:
            user_id: User identifier

        Returns:
            User context dictionary
        """
        try:
            result = self.db.execute_query("""
            SELECT preferences, recent_queries, favorite_dealers, 
                   notification_settings, last_active
            FROM user_context
            WHERE user_id = %s
            """, (user_id,))

            if result and not result.empty:
                row = result.iloc[0]
                return {
                    'preferences': json.loads(row['preferences']) if row['preferences'] else {},
                    'recent_queries': json.loads(row['recent_queries']) if row['recent_queries'] else [],
                    'favorite_dealers': json.loads(row['favorite_dealers']) if row['favorite_dealers'] else [],
                    'notification_settings': json.loads(row['notification_settings']) if row[
                        'notification_settings'] else {},
                    'last_active': row['last_active']
                }

            # Return default context
            return {
                'preferences': {
                    'default_date_range': 'Last 30 Days',
                    'default_metric': 'Revenue',
                    'theme': 'light',
                    'chart_type': 'line'
                },
                'recent_queries': [],
                'favorite_dealers': [],
                'notification_settings': {
                    'alerts_enabled': True,
                    'email_notifications': False,
                    'alert_thresholds': {
                        'margin': 15,
                        'stock_availability': 70,
                        'repair_tat': 48
                    }
                },
                'last_active': None
            }

        except Exception as e:
            logger.error(f"Error getting user context: {str(e)}")
            return {}

    def update_user_context(self, user_id: str, context: Dict) -> bool:
        """
        Update user context

        Args:
            user_id: User identifier
            context: New context data

        Returns:
            Success flag
        """
        try:
            # Get existing context
            existing = self.get_user_context(user_id)

            # Merge with new context
            for key, value in context.items():
                if key in existing:
                    existing[key] = value

            self.db.execute_query("""
            INSERT INTO user_context (user_id, preferences, recent_queries, 
                                       favorite_dealers, notification_settings, last_active)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
            SET preferences = EXCLUDED.preferences,
                recent_queries = EXCLUDED.recent_queries,
                favorite_dealers = EXCLUDED.favorite_dealers,
                notification_settings = EXCLUDED.notification_settings,
                last_active = EXCLUDED.last_active,
                updated_at = CURRENT_TIMESTAMP
            """, (
                user_id,
                json.dumps(existing.get('preferences', {})),
                json.dumps(existing.get('recent_queries', [])),
                json.dumps(existing.get('favorite_dealers', [])),
                json.dumps(existing.get('notification_settings', {})),
                datetime.now()
            ))

            logger.info(f"Updated context for user {user_id}")
            return True

        except Exception as e:
            logger.error(f"Error updating user context: {str(e)}")
            return False

    def add_recent_query(self, user_id: str, query: str, response_summary: str = None) -> bool:
        """
        Add a query to user's recent queries

        Args:
            user_id: User identifier
            query: User question
            response_summary: Summary of response

        Returns:
            Success flag
        """
        try:
            context = self.get_user_context(user_id)
            recent = context.get('recent_queries', [])

            # Add new query
            recent.insert(0, {
                'query': query,
                'response_summary': response_summary,
                'timestamp': datetime.now().isoformat()
            })

            # Keep only last 20 queries
            recent = recent[:20]

            context['recent_queries'] = recent
            return self.update_user_context(user_id, context)

        except Exception as e:
            logger.error(f"Error adding recent query: {str(e)}")
            return False

    def add_favorite_dealer(self, user_id: str, dealer_name: str) -> bool:
        """
        Add a dealer to user's favorites

        Args:
            user_id: User identifier
            dealer_name: Dealer name

        Returns:
            Success flag
        """
        try:
            context = self.get_user_context(user_id)
            favorites = context.get('favorite_dealers', [])

            if dealer_name not in favorites:
                favorites.append(dealer_name)
                context['favorite_dealers'] = favorites
                return self.update_user_context(user_id, context)

            return True

        except Exception as e:
            logger.error(f"Error adding favorite dealer: {str(e)}")
            return False

    def remove_favorite_dealer(self, user_id: str, dealer_name: str) -> bool:
        """
        Remove a dealer from favorites

        Args:
            user_id: User identifier
            dealer_name: Dealer name

        Returns:
            Success flag
        """
        try:
            context = self.get_user_context(user_id)
            favorites = context.get('favorite_dealers', [])

            if dealer_name in favorites:
                favorites.remove(dealer_name)
                context['favorite_dealers'] = favorites
                return self.update_user_context(user_id, context)

            return True

        except Exception as e:
            logger.error(f"Error removing favorite dealer: {str(e)}")
            return False

    def set_preference(self, user_id: str, key: str, value: Any) -> bool:
        """
        Set a user preference

        Args:
            user_id: User identifier
            key: Preference key
            value: Preference value

        Returns:
            Success flag
        """
        try:
            context = self.get_user_context(user_id)
            preferences = context.get('preferences', {})
            preferences[key] = value
            context['preferences'] = preferences
            return self.update_user_context(user_id, context)

        except Exception as e:
            logger.error(f"Error setting preference: {str(e)}")
            return False

    def get_preference(self, user_id: str, key: str, default: Any = None) -> Any:
        """
        Get a user preference

        Args:
            user_id: User identifier
            key: Preference key
            default: Default value if not found

        Returns:
            Preference value
        """
        context = self.get_user_context(user_id)
        return context.get('preferences', {}).get(key, default)

    def update_notification_settings(self, user_id: str, settings: Dict) -> bool:
        """
        Update notification settings

        Args:
            user_id: User identifier
            settings: New notification settings

        Returns:
            Success flag
        """
        try:
            context = self.get_user_context(user_id)
            existing = context.get('notification_settings', {})
            existing.update(settings)
            context['notification_settings'] = existing
            return self.update_user_context(user_id, context)

        except Exception as e:
            logger.error(f"Error updating notification settings: {str(e)}")
            return False