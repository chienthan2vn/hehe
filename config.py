"""
Configuration settings for the RAG pipeline.
"""

import os
from dataclasses import dataclass


@dataclass
class MongoConfig:
    """MongoDB configuration."""
    uri: str = "mongodb://localhost:27001"
    database: str = "pdf_storage"
    collection: str = "pdf_markdown"


@dataclass
class RabbitMQConfig:
    """RabbitMQ configuration."""
    host: str = "localhost"
    port: int = 5673
    queue: str = "chunked_data"
    username: str = "guest"
    password: str = "guest"
    virtual_host: str = "/"


@dataclass
class QdrantConfig:
    """Qdrant configuration."""
    host: str = "localhost"
    port: int = 6333
    collection_name: str = "chunked_vectors"
    vector_size: int = 384  # for all-MiniLM-L6-v2


@dataclass
class EmbeddingConfig:
    """Embedding model configuration."""
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2"


@dataclass
class ChunkingConfig:
    """Text chunking configuration."""
    chunk_size: int = 512
    overlap: int = 50


@dataclass
class PipelineConfig:
    """Main pipeline configuration."""
    input_pdf_dir: str = "src_data"
    
    def __post_init__(self):
        self.mongo = MongoConfig()
        self.rabbitmq = RabbitMQConfig()
        self.qdrant = QdrantConfig()
        self.embedding = EmbeddingConfig()
        self.chunking = ChunkingConfig()


# Global configuration instance
config = PipelineConfig()
