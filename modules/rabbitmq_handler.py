"""
RabbitMQ message queue handler for the pipeline.
"""

import json
import logging
import pika
from typing import Dict, Any, Callable, Optional
from pika.adapters.blocking_connection import BlockingChannel

from config import config

logger = logging.getLogger(__name__)

class RabbitMQHandler:
    """Handles RabbitMQ connections, publishing and consuming."""
    
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
    
    def publish_chunks(self, chunks: list) -> int:
        """
        Publish multiple chunk messages to the queue.
        
        Args:
            chunks: List of chunk dictionaries
            
        Returns:
            Number of successfully published chunks
        """
        published_count = 0
        failed_count = 0
        
        for chunk in chunks:
            if self.publish_message(chunk):
                published_count += 1
            else:
                failed_count += 1
        
        logger.info(f"Published {published_count} chunks, {failed_count} failed")
        return published_count
    
    def setup_consumer(self, callback_function: Callable, auto_ack: bool = False):
        """
        Setup message consumer with callback.
        
        Args:
            callback_function: Function to handle incoming messages
            auto_ack: Whether to auto-acknowledge messages
        """
        try:
            if not self.channel:
                self.setup_connection()
            
            # Set QoS to process one message at a time
            self.channel.basic_qos(prefetch_count=1)
            
            def wrapper_callback(ch, method, properties, body):
                """Wrapper callback to handle message parsing and acknowledgment."""
                try:
                    # Parse JSON message
                    message = json.loads(body.decode('utf-8'))
                    
                    # Call the user-provided callback
                    result = callback_function(message)
                    
                    # Acknowledge message if auto_ack is False and callback succeeded
                    if not auto_ack and result is not False:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        logger.debug(f"Acknowledged message: {message.get('id', 'unknown')}")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse message JSON: {e}")
                    if not auto_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                
                except Exception as e:
                    logger.error(f"Error in message callback: {e}")
                    if not auto_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            self.channel.basic_consume(
                queue=config.rabbitmq.queue,
                on_message_callback=wrapper_callback,
                auto_ack=auto_ack
            )
            
            logger.info("Consumer setup complete")
            
        except Exception as e:
            logger.error(f"Failed to setup consumer: {e}")
            raise
    
    def start_consuming(self):
        """Start consuming messages."""
        try:
            logger.info("Starting to consume messages...")
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
            self.stop_consuming()
        except Exception as e:
            logger.error(f"Error while consuming: {e}")
            raise
    
    def stop_consuming(self):
        """Stop consuming messages."""
        if self.channel:
            self.channel.stop_consuming()
            logger.info("Stopped consuming messages")
    
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
