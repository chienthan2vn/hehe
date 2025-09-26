"""
Bytewax processing pipeline for RAG documents.
Handles chunking, embedding generation, and Qdrant storage.
"""

import json
import logging
import time
from typing import Dict, Any, List, Optional, Iterator, Tuple
from dataclasses import dataclass

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
import pika

from config import config
from modules.text_chunker import TextChunker
from modules.qdrant_handler import QdrantHandler
from modules.rabbitmq_handler import RabbitMQHandler

logger = logging.getLogger(__name__)

@dataclass
class DocumentMessage:
    """Data class for document messages."""
    document_id: str
    file_name: str
    markdown: str
    timestamp: float
    message_type: str = "raw_document"

@dataclass
class ChunkMessage:
    """Data class for chunk messages."""
    chunk_id: str
    document_id: str
    text: str
    chunk_index: int
    word_count: int
    char_count: int
    chunk_method: str
    timestamp: float

class RabbitMQSource:
    """Custom RabbitMQ source for Bytewax."""
    
    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.setup_connection()
    
    def setup_connection(self):
        """Setup RabbitMQ connection."""
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
            
            # Declare queue
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.basic_qos(prefetch_count=1)
            
            logger.info(f"RabbitMQ source connected to queue: {self.queue_name}")
            
        except Exception as e:
            logger.error(f"Failed to setup RabbitMQ source: {e}")
            raise
    
    def __iter__(self) -> Iterator[Tuple[str, DocumentMessage]]:
        """Iterate over messages from RabbitMQ."""
        try:
            for method_frame, properties, body in self.channel.consume(self.queue_name, auto_ack=False):
                if method_frame:
                    try:
                        # Parse message
                        message_data = json.loads(body.decode('utf-8'))
                        
                        # Create DocumentMessage
                        doc_message = DocumentMessage(
                            document_id=message_data.get("document_id", ""),
                            file_name=message_data.get("file_name", ""),
                            markdown=message_data.get("markdown", ""),
                            timestamp=message_data.get("timestamp", time.time()),
                            message_type=message_data.get("message_type", "raw_document")
                        )
                        
                        # Acknowledge message
                        self.channel.basic_ack(method_frame.delivery_tag)
                        
                        # Yield with key (document_id) and value
                        yield (doc_message.document_id, doc_message)
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse message JSON: {e}")
                        # Reject and don't requeue malformed messages
                        self.channel.basic_nack(method_frame.delivery_tag, requeue=False)
                    
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        # Reject and requeue for retry
                        self.channel.basic_nack(method_frame.delivery_tag, requeue=True)
                
        except KeyboardInterrupt:
            logger.info("RabbitMQ source stopped by user")
        except Exception as e:
            logger.error(f"Error in RabbitMQ source: {e}")
        finally:
            self.close()
    
    def close(self):
        """Close RabbitMQ connection."""
        try:
            if self.channel:
                self.channel.close()
            if self.connection:
                self.connection.close()
            logger.info("RabbitMQ source connection closed")
        except Exception as e:
            logger.error(f"Error closing RabbitMQ source: {e}")

class QdrantSink:
    """Custom Qdrant sink for Bytewax."""
    
    def __init__(self):
        self.qdrant_handler = QdrantHandler()
        self.batch_size = 10
        self.batch = []
        
    def write(self, chunk_message: ChunkMessage):
        """Write chunk to Qdrant."""
        try:
            # Convert to dict format expected by Qdrant handler
            chunk_data = {
                "id": chunk_message.chunk_id,
                "text": chunk_message.text,
                "document_id": chunk_message.document_id,
                "chunk_index": chunk_message.chunk_index,
                "word_count": chunk_message.word_count,
                "char_count": chunk_message.char_count,
                "chunk_method": chunk_message.chunk_method
            }
            
            # Add to batch
            self.batch.append(chunk_data)
            
            # Process batch when full
            if len(self.batch) >= self.batch_size:
                self._process_batch()
                
        except Exception as e:
            logger.error(f"Error writing to Qdrant sink: {e}")
    
    def _process_batch(self):
        """Process accumulated batch."""
        if self.batch:
            try:
                stored_count = self.qdrant_handler.store_chunks_batch(self.batch)
                logger.info(f"Stored {stored_count} chunks in batch")
                self.batch.clear()
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
    
    def flush(self):
        """Flush remaining batch."""
        if self.batch:
            self._process_batch()
    
    def close(self):
        """Close sink and flush remaining data."""
        self.flush()
        logger.info("Qdrant sink closed")

def chunk_document(item: Tuple[str, DocumentMessage]) -> List[Tuple[str, ChunkMessage]]:
    """
    Chunk document into smaller pieces.
    
    Args:
        item: Tuple of (document_id, DocumentMessage)
        
    Returns:
        List of chunked messages
    """
    document_id, doc_message = item
    
    try:
        # Initialize text chunker
        chunker = TextChunker()
        
        # Chunk the markdown text
        chunks = chunker.chunk_text_with_metadata(
            text=doc_message.markdown,
            document_id=document_id,
            chunk_method="words"
        )
        
        # Convert to ChunkMessage objects
        chunk_messages = []
        for chunk_data in chunks:
            chunk_message = ChunkMessage(
                chunk_id=chunk_data["id"],
                document_id=chunk_data["document_id"],
                text=chunk_data["text"],
                chunk_index=chunk_data["chunk_index"],
                word_count=chunk_data["word_count"],
                char_count=chunk_data["char_count"],
                chunk_method=chunk_data["chunk_method"],
                timestamp=time.time()
            )
            chunk_messages.append((chunk_data["id"], chunk_message))
        
        logger.info(f"Chunked document {doc_message.file_name} into {len(chunk_messages)} chunks")
        return chunk_messages
        
    except Exception as e:
        logger.error(f"Error chunking document {document_id}: {e}")
        return []

def log_chunk(item: Tuple[str, ChunkMessage]) -> Tuple[str, ChunkMessage]:
    """Log chunk processing."""
    chunk_id, chunk_message = item
    logger.debug(f"Processing chunk: {chunk_id} from document: {chunk_message.document_id}")
    return item

def store_chunk(item: Tuple[str, ChunkMessage]) -> Tuple[str, ChunkMessage]:
    """Store chunk in Qdrant."""
    chunk_id, chunk_message = item
    
    try:
        # Initialize Qdrant handler (should be reused in production)
        qdrant_handler = QdrantHandler()
        
        # Convert to dict format
        chunk_data = {
            "id": chunk_message.chunk_id,
            "text": chunk_message.text,
            "document_id": chunk_message.document_id,
            "chunk_index": chunk_message.chunk_index,
            "word_count": chunk_message.word_count,
            "char_count": chunk_message.char_count,
            "chunk_method": chunk_message.chunk_method
        }
        
        # Store in Qdrant
        success = qdrant_handler.store_chunk(chunk_data)
        
        if success:
            logger.debug(f"Stored chunk: {chunk_id}")
        else:
            logger.error(f"Failed to store chunk: {chunk_id}")
            
        return item
        
    except Exception as e:
        logger.error(f"Error storing chunk {chunk_id}: {e}")
        return item

def create_rag_processing_flow() -> Dataflow:
    """
    Create the main RAG processing Bytewax dataflow.
    
    Returns:
        Configured Bytewax Dataflow
    """
    # Create dataflow
    flow = Dataflow("rag_processing")
    
    # Input: Raw documents from RabbitMQ
    raw_queue = config.rabbitmq.queue + "_raw"
    rabbitmq_source = RabbitMQSource(raw_queue)
    
    # Input stream
    input_stream = op.input("raw_docs", flow, rabbitmq_source)
    
    # Transform 1: Chunk documents
    chunked_stream = op.flat_map("chunk_documents", input_stream, chunk_document)
    
    # Transform 2: Log processing
    logged_stream = op.map("log_chunks", chunked_stream, log_chunk)
    
    # Transform 3: Store in Qdrant
    stored_stream = op.map("store_chunks", logged_stream, store_chunk)
    
    # Output: Log results
    op.output("output", stored_stream, StdOutSink())
    
    return flow

class BytewaxProcessor:
    """Main Bytewax processor class."""
    
    def __init__(self):
        self.flow = create_rag_processing_flow()
        
    def run(self):
        """Run the Bytewax processing pipeline."""
        logger.info("Starting Bytewax RAG processing pipeline...")
        
        try:
            # Run the dataflow (this will block)
            import bytewax.run
            bytewax.run.main(self.flow)
            
        except KeyboardInterrupt:
            logger.info("Bytewax pipeline stopped by user")
        except Exception as e:
            logger.error(f"Error running Bytewax pipeline: {e}")
            raise

# Global flow variable for Bytewax CLI
flow = create_rag_processing_flow()

def main():
    """Main function for testing."""
    processor = BytewaxProcessor()
    processor.run()

if __name__ == "__main__":
    main()
