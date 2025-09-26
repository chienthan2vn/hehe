"""
RabbitMQ message queue handler for the pipeline.
"""

import json
import logging
import pika
from typing import Dict, Any

from config import config

logger = logging.getLogger(__name__)

class RabbitMQHandler:
    """Handles RabbitMQ connections and publishing."""
    
    def __init__(self):
        self.connection = None
        self.channel = None
        self.setup_connection()
    
    def setup_connection(self):
        """Setup RabbitMQ connection and channel."""
        try:
            credentials = pika.PlainCredentials(
                config.rabbitmq.username, 
                config.rabbitmq.password
            )
            
            connection_params = pika.ConnectionParameters(
                host=config.rabbitmq.host,
                port=config.rabbitmq.port,
                virtual_host=config.rabbitmq.virtual_host,
                credentials=credentials
            )
            
            self.connection = pika.BlockingConnection(connection_params)
            self.channel = self.connection.channel()
            
            # Declare the queue with durability
            self.channel.queue_declare(
                queue=config.rabbitmq.queue, 
                durable=True
            )
            
            logger.info("Successfully connected to RabbitMQ")
            
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    def publish_message(self, message: Dict[str, Any]) -> bool:
        """
        Publish a message to the queue.
        
        Args:
            message: Message data to publish
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.channel:
                self.setup_connection()
            
            message_body = json.dumps(message, ensure_ascii=False)
            
            self.channel.basic_publish(
                exchange="",
                routing_key=config.rabbitmq.queue,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )
            )
            
            logger.debug(f"Published message with ID: {message.get('id', 'unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")
            return False
    
    def get_queue_info(self) -> Dict[str, Any]:
        """
        Get information about the queue.
        
        Returns:
            Queue information dictionary
        """
        try:
            if not self.channel:
                self.setup_connection()
            
            method = self.channel.queue_declare(
                queue=config.rabbitmq.queue, 
                passive=True
            )
            
            return {
                "queue_name": config.rabbitmq.queue,
                "message_count": method.method.message_count,
                "consumer_count": method.method.consumer_count
            }
            
        except Exception as e:
            logger.error(f"Failed to get queue info: {e}")
            return {}
    
    def purge_queue(self) -> int:
        """
        Purge all messages from the queue.
        
        Returns:
            Number of messages purged
        """
        try:
            if not self.channel:
                self.setup_connection()
            
            method = self.channel.queue_purge(queue=config.rabbitmq.queue)
            purged_count = method.method.message_count
            
            logger.info(f"Purged {purged_count} messages from queue")
            return purged_count
            
        except Exception as e:
            logger.error(f"Failed to purge queue: {e}")
            return 0
    
    def close(self):
        """Close the connection."""
        try:
            if self.channel:
                self.channel.close()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            logger.info("Closed RabbitMQ connection")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
