"""
Amazon Q Client
Handles interactions with Amazon Q for business intelligence
"""

import boto3
import json
import logging
from typing import Dict, List, Optional, Any
from botocore.exceptions import ClientError
import time

logger = logging.getLogger(__name__)


class AmazonQClient:
    """Client for Amazon Q business intelligence"""

    def __init__(self, aws_config: Dict):
        """
        Initialize Amazon Q client

        Args:
            aws_config: AWS configuration dictionary
        """
        self.region = aws_config.get('region', 'us-east-1')
        self.application_id = aws_config.get('amazon_q_application_id')

        # Initialize boto3 clients
        self.q_client = boto3.client(
            service_name='qbusiness',
            region_name=self.region
        )

        logger.info(f"Initialized Amazon Q client")

    def ask_question(self, question: str, conversation_id: str = None) -> Dict:
        """
        Ask a question to Amazon Q

        Args:
            question: User question
            conversation_id: Optional conversation ID for context

        Returns:
            Response dictionary with answer and sources
        """
        try:
            request = {
                'applicationId': self.application_id,
                'userMessage': question
            }

            if conversation_id:
                request['conversationId'] = conversation_id

            response = self.q_client.chat_sync(**request)

            return {
                'answer': response.get('systemMessage', ''),
                'conversation_id': response.get('conversationId'),
                'sources': response.get('sourceAttributions', [])
            }

        except ClientError as e:
            logger.error(f"Amazon Q API error: {str(e)}")
            return {
                'answer': f"Error: {str(e)}",
                'conversation_id': conversation_id,
                'sources': []
            }
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {
                'answer': f"Error: {str(e)}",
                'conversation_id': conversation_id,
                'sources': []
            }

    def create_conversation(self) -> str:
        """
        Create a new conversation

        Returns:
            Conversation ID
        """
        try:
            response = self.q_client.create_conversation(
                applicationId=self.application_id
            )

            return response.get('conversationId', '')

        except Exception as e:
            logger.error(f"Error creating conversation: {str(e)}")
            return ''

    def delete_conversation(self, conversation_id: str) -> bool:
        """
        Delete a conversation

        Args:
            conversation_id: ID of conversation to delete

        Returns:
            Success flag
        """
        try:
            self.q_client.delete_conversation(
                applicationId=self.application_id,
                conversationId=conversation_id
            )
            return True

        except Exception as e:
            logger.error(f"Error deleting conversation: {str(e)}")
            return False

    def list_conversations(self) -> List[Dict]:
        """
        List all conversations

        Returns:
            List of conversation metadata
        """
        try:
            response = self.q_client.list_conversations(
                applicationId=self.application_id
            )

            conversations = []
            for conv in response.get('conversations', []):
                conversations.append({
                    'conversation_id': conv.get('conversationId'),
                    'title': conv.get('title', ''),
                    'created_at': conv.get('createTime'),
                    'updated_at': conv.get('updateTime')
                })

            return conversations

        except Exception as e:
            logger.error(f"Error listing conversations: {str(e)}")
            return []

    def get_conversation_messages(self, conversation_id: str) -> List[Dict]:
        """
        Get messages from a conversation

        Args:
            conversation_id: Conversation ID

        Returns:
            List of messages
        """
        try:
            response = self.q_client.get_conversation(
                applicationId=self.application_id,
                conversationId=conversation_id
            )

            messages = []
            for msg in response.get('messages', []):
                messages.append({
                    'id': msg.get('messageId'),
                    'content': msg.get('body', ''),
                    'role': msg.get('type', ''),
                    'created_at': msg.get('time')
                })

            return messages

        except Exception as e:
            logger.error(f"Error getting conversation messages: {str(e)}")
            return []

    def send_feedback(self, message_id: str, feedback_type: str) -> bool:
        """
        Send feedback on a response

        Args:
            message_id: Message ID
            feedback_type: 'positive' or 'negative'

        Returns:
            Success flag
        """
        try:
            self.q_client.put_feedback(
                applicationId=self.application_id,
                messageId=message_id,
                feedbackType=feedback_type
            )
            return True

        except Exception as e:
            logger.error(f"Error sending feedback: {str(e)}")
            return False