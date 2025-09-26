"""
Qdrant vector database handler for storing and retrieving embeddings.
"""

import logging
from typing import Dict, Any
from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams, Distance
from sentence_transformers import SentenceTransformer

from config import config

logger = logging.getLogger(__name__)

class QdrantHandler:
    """Handles Qdrant vector database operations."""
    
    def __init__(self):
        self.client = QdrantClient(
            host=config.qdrant.host,
            port=config.qdrant.port
        )
        self.collection_name = config.qdrant.collection_name
        self.vector_size = config.qdrant.vector_size
        
        # Initialize embedding model (needed by Bytewax)
        self.embedding_model = SentenceTransformer(config.embedding.model_name)
        
        # Ensure collection exists
        self.ensure_collection_exists()
    
    def ensure_collection_exists(self):
        """Create collection if it doesn't exist."""
        try:
            # Check if collection exists
            collections = self.client.get_collections()
            collection_names = [col.name for col in collections.collections]
            
            if self.collection_name not in collection_names:
                # Create collection
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=self.vector_size,
                        distance=Distance.COSINE
                    )
                )
                logger.info(f"Created Qdrant collection: {self.collection_name}")
            else:
                logger.info(f"Qdrant collection already exists: {self.collection_name}")
                
        except Exception as e:
            logger.error(f"Failed to ensure collection exists: {e}")
            raise
    
    def get_collection_info(self) -> Dict[str, Any]:
        """
        Get information about the collection.
        
        Returns:
            Collection information dictionary
        """
        try:
            info = self.client.get_collection(self.collection_name)
            return {
                "name": info.name,
                "vectors_count": info.vectors_count,
                "indexed_vectors_count": info.indexed_vectors_count,
                "points_count": info.points_count,
                "segments_count": info.segments_count,
                "status": info.status,
                "optimizer_status": info.optimizer_status
            }
        except Exception as e:
            logger.error(f"Failed to get collection info: {e}")
            return {}
