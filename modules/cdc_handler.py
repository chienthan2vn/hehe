"""
Change Data Capture (CDC) handler for MongoDB to RabbitMQ.
Sends raw document data without chunking.
"""

import logging
import json
import time
from typing import Dict, Any, List, Optional
from pymongo import MongoClient
from pymongo.collection import Collection
from bson import ObjectId

from config import config
from modules.rabbitmq_handler import RabbitMQHandler

logger = logging.getLogger(__name__)

class CDCHandler:
    """
    Handles Change Data Capture from MongoDB to RabbitMQ.
    Sends raw document data for processing by Bytewax pipeline.
    """
    
    def __init__(self):
        self.mongo_client = MongoClient(config.mongo.uri)
        self.db = self.mongo_client[config.mongo.database]
        self.collection = self.db[config.mongo.collection]
        self.rabbitmq_handler = RabbitMQHandler()
        
        # Use different queue for raw documents
        self.raw_queue = config.rabbitmq.queue + "_raw"
        
        # Ensure raw queue exists
        self._setup_raw_queue()
    
    def _setup_raw_queue(self):
        """Setup the raw document queue."""
        try:
            # Declare raw queue
            self.rabbitmq_handler.channel.queue_declare(
                queue=self.raw_queue, 
                durable=True
            )
            logger.info(f"Raw document queue setup: {self.raw_queue}")
        except Exception as e:
            logger.error(f"Failed to setup raw queue: {e}")
            raise
    
    def send_raw_document(self, document: Dict[str, Any]) -> bool:
        """
        Send raw document to RabbitMQ for Bytewax processing.
        
        Args:
            document: MongoDB document
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare raw document message
            raw_message = {
                "document_id": str(document["_id"]),
                "file_name": document.get("file_name", ""),
                "file_path": document.get("file_path", ""),
                "markdown": document.get("markdown", ""),
                "created_at": document.get("created_at", "").isoformat() if document.get("created_at") else None,
                "message_type": "raw_document",
                "timestamp": time.time()
            }
            
            # Send to raw queue
            message_body = json.dumps(raw_message, ensure_ascii=False, default=str)
            
            success = self.rabbitmq_handler.channel.basic_publish(
                exchange="",
                routing_key=self.raw_queue,
                body=message_body,
                properties=self.rabbitmq_handler.connection.BasicProperties(
                    delivery_mode=2,  # Persistent
                    content_type='application/json'
                )
            )
            
            if success:
                logger.debug(f"Sent raw document: {document['file_name']}")
                return True
            else:
                logger.error(f"Failed to send raw document: {document['file_name']}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending raw document: {e}")
            return False
    
    def process_unprocessed_documents(self) -> int:
        """
        Process all unprocessed documents and send to RabbitMQ.
        
        Returns:
            Number of documents sent
        """
        try:
            # Get unprocessed documents
            unprocessed_docs = list(self.collection.find({"processed": False}))
            
            if not unprocessed_docs:
                logger.info("No unprocessed documents found")
                return 0
            
            logger.info(f"Processing {len(unprocessed_docs)} unprocessed documents")
            
            sent_count = 0
            
            for doc in unprocessed_docs:
                if self.send_raw_document(doc):
                    sent_count += 1
                    
                    # Mark as processed (CDC processed, not chunked)
                    self.collection.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"processed": True, "cdc_processed_at": time.time()}}
                    )
                    
                    logger.debug(f"Marked document as CDC processed: {doc['file_name']}")
                else:
                    logger.error(f"Failed to send document: {doc['file_name']}")
            
            logger.info(f"CDC processing completed. Sent {sent_count}/{len(unprocessed_docs)} documents")
            return sent_count
            
        except Exception as e:
            logger.error(f"Error in CDC processing: {e}")
            return 0
    
    def watch_changes(self, callback: Optional[callable] = None):
        """
        Watch for changes in MongoDB collection and send new documents.
        
        Args:
            callback: Optional callback function for each change
        """
        try:
            logger.info("Starting MongoDB change stream watch...")
            
            # Watch for insert operations
            change_stream = self.collection.watch([
                {"$match": {"operationType": "insert"}}
            ])
            
            for change in change_stream:
                try:
                    document = change["fullDocument"]
                    logger.info(f"New document detected: {document.get('file_name', 'unknown')}")
                    
                    # Send to RabbitMQ
                    if self.send_raw_document(document):
                        # Mark as CDC processed
                        self.collection.update_one(
                            {"_id": document["_id"]},
                            {"$set": {"processed": True, "cdc_processed_at": time.time()}}
                        )
                        
                        if callback:
                            callback(document)
                    
                except Exception as e:
                    logger.error(f"Error processing change event: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Change stream watch stopped by user")
        except Exception as e:
            logger.error(f"Error in change stream: {e}")
        finally:
            change_stream.close()
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the raw document queue.
        
        Returns:
            Queue statistics
        """
        try:
            method = self.rabbitmq_handler.channel.queue_declare(
                queue=self.raw_queue, 
                passive=True
            )
            
            return {
                "queue_name": self.raw_queue,
                "message_count": method.method.message_count,
                "consumer_count": method.method.consumer_count
            }
            
        except Exception as e:
            logger.error(f"Failed to get queue stats: {e}")
            return {}
    
    def purge_raw_queue(self) -> int:
        """
        Purge all messages from raw document queue.
        
        Returns:
            Number of messages purged
        """
        try:
            method = self.rabbitmq_handler.channel.queue_purge(queue=self.raw_queue)
            purged_count = method.method.message_count
            
            logger.info(f"Purged {purged_count} messages from raw queue")
            return purged_count
            
        except Exception as e:
            logger.error(f"Failed to purge raw queue: {e}")
            return 0
    
    def close(self):
        """Close connections."""
        try:
            self.mongo_client.close()
            self.rabbitmq_handler.close()
            logger.info("CDC Handler connections closed")
        except Exception as e:
            logger.error(f"Error closing CDC Handler: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
