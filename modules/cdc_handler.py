"""
Change Data Capture (CDC) handler for MongoDB to RabbitMQ.
Simply sends raw document data to queue for Bytewax processing.
"""

import logging
import time
from typing import Dict, Any, Optional, Callable

from config import config
from modules.rabbitmq_handler import RabbitMQHandler
from modules.pdf_extractor import PDFExtractor

logger = logging.getLogger(__name__)

class CDCHandler:
    """
    Simple CDC handler: MongoDB → RabbitMQ → Bytewax
    Only responsible for detecting changes and sending to queue.
    """
    
    def __init__(self):
        self.pdf_extractor = PDFExtractor()
        self.rabbitmq_handler = RabbitMQHandler()
        
        # Raw queue for unprocessed documents
        self.raw_queue = config.rabbitmq.queue + "_raw"
        
        logger.info(f"CDC Handler initialized with raw queue: {self.raw_queue}")
    
    def send_to_queue(self, document: Dict[str, Any]) -> bool:
        """
        Send raw document to queue for Bytewax processing.
        
        Args:
            document: MongoDB document
            
        Returns:
            True if successful
        """
        try:
            # Simple message format for Bytewax
            raw_message = {
                "document_id": str(document["_id"]),
                "file_name": document.get("file_name", ""),
                "markdown": document.get("markdown", ""),
                "message_type": "raw_document",
                "timestamp": time.time()
            }
            
            # Use helper method to publish to specific queue
            success = self._publish_to_raw_queue(raw_message)
            
            if success:
                logger.debug(f"Sent to queue: {document.get('file_name', 'unknown')}")
            
            return success
                
        except Exception as e:
            logger.error(f"Error sending to queue: {e}")
            return False
    
    def _publish_to_raw_queue(self, message: Dict[str, Any]) -> bool:
        """Helper method to publish to raw queue."""
        original_queue = config.rabbitmq.queue
        config.rabbitmq.queue = self.raw_queue
        try:
            return self.rabbitmq_handler.publish_message(message)
        finally:
            config.rabbitmq.queue = original_queue
    
    def process_batch(self) -> int:
        """
        Send all unprocessed documents to queue.
        
        Returns:
            Number of documents sent
        """
        try:
            unprocessed_docs = self.pdf_extractor.get_unprocessed_documents()
            
            if not unprocessed_docs:
                logger.info("No unprocessed documents found")
                return 0
            
            logger.info(f"Sending {len(unprocessed_docs)} documents to queue")
            
            sent_count = 0
            for doc in unprocessed_docs:
                if self.send_to_queue(doc):
                    sent_count += 1
            
            logger.info(f"CDC batch complete: {sent_count}/{len(unprocessed_docs)} sent")
            return sent_count
            
        except Exception as e:
            logger.error(f"CDC batch processing error: {e}")
            return 0
    
    def watch_realtime(self, callback: Optional[Callable] = None):
        """
        Watch for new documents in MongoDB and send to queue.
        
        Args:
            callback: Optional callback for each document
        """
        try:
            logger.info("Starting CDC realtime watch...")
            
            change_stream = self.pdf_extractor.collection.watch([
                {"$match": {"operationType": "insert"}}
            ])
            
            for change in change_stream:
                try:
                    document = change["fullDocument"]
                    logger.info(f"New document detected: {document.get('file_name', 'unknown')}")
                    
                    if self.send_to_queue(document):
                        if callback:
                            callback(document)
                    
                except Exception as e:
                    logger.error(f"Error processing change: {e}")
                    
        except KeyboardInterrupt:
            logger.info("CDC watch stopped")
        except Exception as e:
            logger.error(f"CDC watch error: {e}")
        finally:
            change_stream.close()
    
    def get_queue_stats(self) -> Dict[str, Any]:
        """Get raw queue statistics."""
        try:
            return self._execute_on_raw_queue(self.rabbitmq_handler.get_queue_info)
        except Exception as e:
            logger.error(f"Failed to get queue stats: {e}")
            return {}
    
    def purge_raw_queue(self) -> int:
        """Purge raw queue."""
        try:
            return self._execute_on_raw_queue(self.rabbitmq_handler.purge_queue)
        except Exception as e:
            logger.error(f"Failed to purge queue: {e}")
            return 0
    
    def _execute_on_raw_queue(self, func):
        """Helper method to execute function on raw queue."""
        original_queue = config.rabbitmq.queue
        config.rabbitmq.queue = self.raw_queue
        try:
            return func()
        finally:
            config.rabbitmq.queue = original_queue
    
    def close(self):
        """Close connections."""
        try:
            self.pdf_extractor.close()
            self.rabbitmq_handler.close()
            logger.info("CDC Handler closed")
        except Exception as e:
            logger.error(f"Error closing CDC Handler: {e}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
