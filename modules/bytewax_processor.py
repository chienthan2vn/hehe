"""
Bytewax processing pipeline for RAG documents.
Handles chunking, embedding generation, and Qdrant storage.
"""

import json
import logging
import time
from typing import Dict, Any, List, Tuple
from dataclasses import dataclass

import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow

from config import config
from modules.text_chunker import TextChunker
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
    """Custom RabbitMQ source for Bytewax - reuses RabbitMQHandler."""
    
    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        # Temporarily change config to target specific queue
        original_queue = config.rabbitmq.queue
        config.rabbitmq.queue = queue_name
        
        self.rabbitmq_handler = RabbitMQHandler()
        
        # Restore original config
        config.rabbitmq.queue = original_queue
        
        # Setup consumer-specific configurations
        self.rabbitmq_handler.channel.basic_qos(prefetch_count=1)
    
    def __iter__(self):
        """Iterate over messages from RabbitMQ."""
        try:
            for method_frame, properties, body in self.rabbitmq_handler.channel.consume(self.queue_name, auto_ack=False):
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
                        self.rabbitmq_handler.channel.basic_ack(method_frame.delivery_tag)
                        
                        # Yield with key (document_id) and value
                        yield (doc_message.document_id, doc_message)
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse message JSON: {e}")
                        self.rabbitmq_handler.channel.basic_nack(method_frame.delivery_tag, requeue=False)
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        self.rabbitmq_handler.channel.basic_nack(method_frame.delivery_tag, requeue=True)
                
        except KeyboardInterrupt:
            logger.info("RabbitMQ source stopped by user")
        except Exception as e:
            logger.error(f"Error in RabbitMQ source: {e}")
        finally:
            self.close()
    
    def close(self):
        """Close RabbitMQ connection."""
        self.rabbitmq_handler.close()

def chunk_document(item: Tuple[str, DocumentMessage]) -> List[Tuple[str, ChunkMessage]]:
    """Chunk document into smaller pieces."""
    document_id, doc_message = item
    
    try:
        chunker = TextChunker()
        chunks = chunker.chunk_text_with_metadata(
            text=doc_message.markdown,
            document_id=document_id,
            chunk_method="words"
        )
        
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

def store_chunk(item: Tuple[str, ChunkMessage]) -> Tuple[str, ChunkMessage]:
    """Store chunk in Qdrant - reuses QdrantHandler."""
    chunk_id, chunk_message = item
    
    try:
        # Import here to avoid circular dependency
        from modules.qdrant_handler import QdrantHandler
        from qdrant_client.http.models import PointStruct
        
        # Reuse QdrantHandler
        qdrant_handler = QdrantHandler()
        
        # Generate embedding using handler's model
        embedding = qdrant_handler.embedding_model.encode(chunk_message.text).tolist()
        
        # Create point
        point = PointStruct(
            id=chunk_message.chunk_id,
            vector=embedding,
            payload={
                "text": chunk_message.text,
                "document_id": chunk_message.document_id,
                "chunk_index": chunk_message.chunk_index,
                "word_count": chunk_message.word_count,
                "char_count": chunk_message.char_count,
                "chunk_method": chunk_message.chunk_method
            }
        )
        
        # Store in Qdrant using handler's client
        qdrant_handler.client.upsert(
            collection_name=qdrant_handler.collection_name,
            points=[point]
        )
        
        logger.debug(f"Stored chunk: {chunk_id}")
        return item
        
    except Exception as e:
        logger.error(f"Error storing chunk {chunk_id}: {e}")
        return item

def create_rag_processing_flow() -> Dataflow:
    """Create the main RAG processing Bytewax dataflow."""
    flow = Dataflow("rag_processing")
    
    # Input: Raw documents from RabbitMQ
    raw_queue = config.rabbitmq.queue + "_raw"
    rabbitmq_source = RabbitMQSource(raw_queue)
    
    # Input stream
    input_stream = op.input("raw_docs", flow, rabbitmq_source)
    
    # Transform 1: Chunk documents
    chunked_stream = op.flat_map("chunk_documents", input_stream, chunk_document)
    
    # Transform 2: Store in Qdrant
    stored_stream = op.map("store_chunks", chunked_stream, store_chunk)
    
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
            import bytewax.run
            bytewax.run.main(self.flow)
        except KeyboardInterrupt:
            logger.info("Bytewax pipeline stopped by user")
        except Exception as e:
            logger.error(f"Error running Bytewax pipeline: {e}")
            raise

# Global flow variable for Bytewax CLI
flow = create_rag_processing_flow()
