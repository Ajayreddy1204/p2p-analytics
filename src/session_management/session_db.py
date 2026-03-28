"""
Session Database Module
Manages user sessions in the database
"""

import json
import uuid
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd

logger = logging.getLogger(__name__)


class SessionManager:
    """Manage user sessions in the database"""

    def __init__(self, db_connection):
        """
        Initialize session manager

        Args:
            db_connection: Database connection object
        """
        self.db = db_connection
        self._create_tables()

    def _create_tables(self):
        """Create session tables if they don't exist"""
        try:
            # User sessions table
            self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                session_id VARCHAR(255) PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                session_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
            """)

            # Chat history table
            self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS chat_history (
                id SERIAL PRIMARY KEY,
                session_id VARCHAR(255) NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                role VARCHAR(50) NOT NULL,
                content TEXT NOT NULL,
                response_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (session_id) REFERENCES user_sessions(session_id)
            )
            """)

            # Query cache table
            self.db.execute_query("""
            CREATE TABLE IF NOT EXISTS query_cache (
                query_hash VARCHAR(255) PRIMARY KEY,
                query_text TEXT NOT NULL,
                sql_query TEXT,
                response_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                hit_count INT DEFAULT 0
            )
            """)

            # Create indexes
            self.db.execute_query("""
            CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id 
            ON user_sessions(user_id)
            """)

            self.db.execute_query("""
            CREATE INDEX IF NOT EXISTS idx_chat_history_session_id 
            ON chat_history(session_id)
            """)

            self.db.execute_query("""
            CREATE INDEX IF NOT EXISTS idx_query_cache_expires_at 
            ON query_cache(expires_at)
            """)

            logger.info("Session tables created/verified")

        except Exception as e:
            logger.error(f"Error creating session tables: {str(e)}")

    def create_session(self, user_id: str, session_data: Dict = None) -> str:
        """
        Create a new user session

        Args:
            user_id: User identifier
            session_data: Optional session data

        Returns:
            Session ID
        """
        session_id = str(uuid.uuid4())
        expires_at = datetime.now() + timedelta(days=7)

        session_data = session_data or {}

        try:
            self.db.execute_query("""
            INSERT INTO user_sessions (session_id, user_id, session_data, expires_at)
            VALUES (%s, %s, %s, %s)
            """, (session_id, user_id, json.dumps(session_data), expires_at))

            logger.info(f"Created session {session_id} for user {user_id}")
            return session_id

        except Exception as e:
            logger.error(f"Error creating session: {str(e)}")
            return ''

    def get_session(self, session_id: str) -> Optional[Dict]:
        """
        Get session data

        Args:
            session_id: Session ID

        Returns:
            Session dictionary or None
        """
        try:
            result = self.db.execute_query("""
            SELECT session_id, user_id, session_data, created_at, updated_at, expires_at, is_active
            FROM user_sessions
            WHERE session_id = %s
            """, (session_id,))

            if result and not result.empty:
                row = result.iloc[0]
                return {
                    'session_id': row['session_id'],
                    'user_id': row['user_id'],
                    'session_data': json.loads(row['session_data']) if row['session_data'] else {},
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at'],
                    'expires_at': row['expires_at'],
                    'is_active': row['is_active']
                }

            return None

        except Exception as e:
            logger.error(f"Error getting session: {str(e)}")
            return None

    def update_session(self, session_id: str, session_data: Dict) -> bool:
        """
        Update session data

        Args:
            session_id: Session ID
            session_data: New session data

        Returns:
            Success flag
        """
        try:
            self.db.execute_query("""
            UPDATE user_sessions
            SET session_data = %s, updated_at = CURRENT_TIMESTAMP
            WHERE session_id = %s
            """, (json.dumps(session_data), session_id))

            return True

        except Exception as e:
            logger.error(f"Error updating session: {str(e)}")
            return False

    def delete_session(self, session_id: str) -> bool:
        """
        Delete a session

        Args:
            session_id: Session ID

        Returns:
            Success flag
        """
        try:
            self.db.execute_query("""
            DELETE FROM user_sessions WHERE session_id = %s
            """, (session_id,))

            return True

        except Exception as e:
            logger.error(f"Error deleting session: {str(e)}")
            return False

    def add_chat_message(self, session_id: str, user_id: str, role: str,
                         content: str, response_data: Dict = None) -> int:
        """
        Add a chat message to history

        Args:
            session_id: Session ID
            user_id: User ID
            role: Message role (user, assistant)
            content: Message content
            response_data: Optional response data

        Returns:
            Message ID
        """
        try:
            result = self.db.execute_query("""
            INSERT INTO chat_history (session_id, user_id, role, content, response_data)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """, (session_id, user_id, role, content, json.dumps(response_data) if response_data else None))

            if result and not result.empty:
                return result['id'].iloc[0]

            return 0

        except Exception as e:
            logger.error(f"Error adding chat message: {str(e)}")
            return 0

    def get_chat_history(self, session_id: str, limit: int = 50) -> List[Dict]:
        """
        Get chat history for a session

        Args:
            session_id: Session ID
            limit: Maximum number of messages

        Returns:
            List of message dictionaries
        """
        try:
            result = self.db.execute_query("""
            SELECT id, role, content, response_data, created_at
            FROM chat_history
            WHERE session_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """, (session_id, limit))

            messages = []
            if result and not result.empty:
                for _, row in result.iterrows():
                    messages.append({
                        'id': row['id'],
                        'role': row['role'],
                        'content': row['content'],
                        'response_data': json.loads(row['response_data']) if row['response_data'] else None,
                        'created_at': row['created_at']
                    })

            return messages

        except Exception as e:
            logger.error(f"Error getting chat history: {str(e)}")
            return []

    def cache_query(self, query_hash: str, query_text: str, sql_query: str,
                    response_data: Dict, ttl_hours: int = 24) -> bool:
        """
        Cache a query result

        Args:
            query_hash: Hash of the query
            query_text: Original question text
            sql_query: SQL query executed
            response_data: Response data to cache
            ttl_hours: Time to live in hours

        Returns:
            Success flag
        """
        try:
            expires_at = datetime.now() + timedelta(hours=ttl_hours)

            self.db.execute_query("""
            INSERT INTO query_cache (query_hash, query_text, sql_query, response_data, expires_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (query_hash) DO UPDATE
            SET response_data = EXCLUDED.response_data,
                expires_at = EXCLUDED.expires_at,
                hit_count = query_cache.hit_count + 1
            """, (query_hash, query_text, sql_query, json.dumps(response_data), expires_at))

            return True

        except Exception as e:
            logger.error(f"Error caching query: {str(e)}")
            return False

    def get_cached_query(self, query_hash: str) -> Optional[Dict]:
        """
        Get cached query result

        Args:
            query_hash: Hash of the query

        Returns:
            Cached response or None
        """
        try:
            result = self.db.execute_query("""
            SELECT query_text, sql_query, response_data, hit_count, created_at
            FROM query_cache
            WHERE query_hash = %s AND expires_at > CURRENT_TIMESTAMP
            """, (query_hash,))

            if result and not result.empty:
                row = result.iloc[0]
                return {
                    'query_text': row['query_text'],
                    'sql_query': row['sql_query'],
                    'response_data': json.loads(row['response_data']) if row['response_data'] else None,
                    'hit_count': row['hit_count'],
                    'cached_at': row['created_at']
                }

            return None

        except Exception as e:
            logger.error(f"Error getting cached query: {str(e)}")
            return None

    def clean_expired_sessions(self):
        """Delete expired sessions"""
        try:
            self.db.execute_query("""
            DELETE FROM user_sessions
            WHERE expires_at < CURRENT_TIMESTAMP OR is_active = FALSE
            """)

            logger.info("Cleaned expired sessions")

        except Exception as e:
            logger.error(f"Error cleaning expired sessions: {str(e)}")

    def clean_expired_cache(self):
        """Delete expired cache entries"""
        try:
            self.db.execute_query("""
            DELETE FROM query_cache
            WHERE expires_at < CURRENT_TIMESTAMP
            """)

            logger.info("Cleaned expired cache")

        except Exception as e:
            logger.error(f"Error cleaning expired cache: {str(e)}")