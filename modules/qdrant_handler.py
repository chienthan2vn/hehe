"""
Qdrant vector database handler for storing and retrieving embeddings.
"""

import logging
from typing import List, Dict, Any, Optional
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, VectorParams, Distance
from qdrant_client.http.exceptions import ResponseHandlingException
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
        
        # Initialize embedding model
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
    
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate embedding for text.
        
        Args:
            text: Input text
            
        Returns:
            Embedding vector
        """
        try:
            embedding = self.embedding_model.encode(text)
            return embedding.tolist()
        except Exception as e:
            logger.error(f"Failed to generate embedding: {e}")
            raise
    
    def store_chunk(self, chunk_data: Dict[str, Any]) -> bool:
        """
        Store a single chunk with its embedding.
        
        Args:
            chunk_data: Chunk data dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            text = chunk_data.get("text", "")
            chunk_id = chunk_data.get("id", "")
            
            if not text or not chunk_id:
                logger.error("Missing text or chunk_id in chunk data")
                return False
            
            # Generate embedding
            embedding = self.generate_embedding(text)
            
            # Prepare payload (metadata)
            payload = {
                "text": text,
                "document_id": chunk_data.get("document_id", ""),
                "chunk_index": chunk_data.get("chunk_index", 0),
                "word_count": chunk_data.get("word_count", 0),
                "char_count": chunk_data.get("char_count", 0),
                "chunk_method": chunk_data.get("chunk_method", "words")
            }
            
            # Create point
            point = PointStruct(
                id=chunk_id,
                vector=embedding,
                payload=payload
            )
            
            # Upsert point
            self.client.upsert(
                collection_name=self.collection_name,
                points=[point]
            )
            
            logger.debug(f"Stored chunk: {chunk_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store chunk {chunk_data.get('id', 'unknown')}: {e}")
            return False
    
    def store_chunks_batch(self, chunks: List[Dict[str, Any]], batch_size: int = 100) -> int:
        """
        Store multiple chunks in batches.
        
        Args:
            chunks: List of chunk dictionaries
            batch_size: Number of chunks to process per batch
            
        Returns:
            Number of successfully stored chunks
        """
        total_stored = 0
        total_chunks = len(chunks)
        
        logger.info(f"Storing {total_chunks} chunks in batches of {batch_size}")
        
        for i in range(0, total_chunks, batch_size):
            batch = chunks[i:i + batch_size]
            batch_stored = 0
            
            try:
                points = []
                for chunk_data in batch:
                    text = chunk_data.get("text", "")
                    chunk_id = chunk_data.get("id", "")
                    
                    if not text or not chunk_id:
                        logger.warning(f"Skipping chunk with missing text or ID")
                        continue
                    
                    # Generate embedding
                    embedding = self.generate_embedding(text)
                    
                    # Prepare payload
                    payload = {
                        "text": text,
                        "document_id": chunk_data.get("document_id", ""),
                        "chunk_index": chunk_data.get("chunk_index", 0),
                        "word_count": chunk_data.get("word_count", 0),
                        "char_count": chunk_data.get("char_count", 0),
                        "chunk_method": chunk_data.get("chunk_method", "words")
                    }
                    
                    points.append(PointStruct(
                        id=chunk_id,
                        vector=embedding,
                        payload=payload
                    ))
                
                # Batch upsert
                if points:
                    self.client.upsert(
                        collection_name=self.collection_name,
                        points=points
                    )
                    batch_stored = len(points)
                    total_stored += batch_stored
                
                logger.info(f"Batch {i//batch_size + 1}: stored {batch_stored}/{len(batch)} chunks")
                
            except Exception as e:
                logger.error(f"Failed to store batch {i//batch_size + 1}: {e}")
        
        logger.info(f"Successfully stored {total_stored}/{total_chunks} chunks")
        return total_stored
    
    def search_similar(self, query: str, limit: int = 10, score_threshold: float = 0.0) -> List[Dict[str, Any]]:
        """
        Search for similar chunks.
        
        Args:
            query: Search query text
            limit: Maximum number of results
            score_threshold: Minimum similarity score
            
        Returns:
            List of similar chunks with scores
        """
        try:
            # Generate query embedding
            query_embedding = self.generate_embedding(query)
            
            # Search
            search_result = self.client.search(
                collection_name=self.collection_name,
                query_vector=query_embedding,
                limit=limit,
                score_threshold=score_threshold
            )
            
            results = []
            for point in search_result:
                result = {
                    "id": point.id,
                    "score": point.score,
                    "text": point.payload.get("text", ""),
                    "document_id": point.payload.get("document_id", ""),
                    "chunk_index": point.payload.get("chunk_index", 0),
                    "word_count": point.payload.get("word_count", 0),
                    "chunk_method": point.payload.get("chunk_method", "")
                }
                results.append(result)
            
            logger.info(f"Found {len(results)} similar chunks for query")
            return results
            
        except Exception as e:
            logger.error(f"Failed to search similar chunks: {e}")
            return []
    
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
    
    def delete_chunk(self, chunk_id: str) -> bool:
        """
        Delete a chunk by ID.
        
        Args:
            chunk_id: Chunk ID to delete
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.delete(
                collection_name=self.collection_name,
                points_selector=[chunk_id]
            )
            logger.info(f"Deleted chunk: {chunk_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete chunk {chunk_id}: {e}")
            return False
    
    def delete_by_document_id(self, document_id: str) -> int:
        """
        Delete all chunks for a specific document.
        
        Args:
            document_id: Document ID
            
        Returns:
            Number of deleted chunks
        """
        try:
            # First, search for all points with this document_id
            search_result = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter={
                    "must": [{"key": "document_id", "match": {"value": document_id}}]
                },
                limit=10000  # Large limit to get all chunks
            )
            
            point_ids = [point.id for point in search_result[0]]
            
            if point_ids:
                self.client.delete(
                    collection_name=self.collection_name,
                    points_selector=point_ids
                )
                logger.info(f"Deleted {len(point_ids)} chunks for document: {document_id}")
                return len(point_ids)
            else:
                logger.info(f"No chunks found for document: {document_id}")
                return 0
                
        except Exception as e:
            logger.error(f"Failed to delete chunks for document {document_id}: {e}")
            return 0
    
    def clear_collection(self) -> bool:
        """
        Clear all data from the collection.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.delete_collection(self.collection_name)
            self.ensure_collection_exists()  # Recreate empty collection
            logger.info(f"Cleared collection: {self.collection_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to clear collection: {e}")
            return False
