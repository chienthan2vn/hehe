"""
Text chunking module for splitting text into smaller segments.
"""

import logging
from typing import List, Dict, Any, Generator

from config import config

logger = logging.getLogger(__name__)

class TextChunker:
    """Handles text chunking with configurable size and overlap."""
    
    def __init__(self, chunk_size: int = None, overlap: int = None):
        self.chunk_size = chunk_size or config.chunking.chunk_size
        self.overlap = overlap or config.chunking.overlap
        
        if self.overlap >= self.chunk_size:
            raise ValueError("Overlap must be smaller than chunk size")
    
    def chunk_by_words(self, text: str) -> Generator[str, None, None]:
        """
        Split text into chunks by words.
        
        Args:
            text: Input text to chunk
            
        Yields:
            Text chunks
        """
        if not text or not text.strip():
            return
        
        words = text.split()
        if len(words) <= self.chunk_size:
            yield text
            return
        
        start = 0
        while start < len(words):
            end = min(start + self.chunk_size, len(words))
            chunk = " ".join(words[start:end])
            yield chunk
            
            # Move start position with overlap consideration
            if end == len(words):  # Last chunk
                break
            start = end - self.overlap if self.overlap > 0 else end
    
    def chunk_text_with_metadata(self, text: str, document_id: str, 
                                 chunk_method: str = "words") -> List[Dict[str, Any]]:
        """
        Chunk text and return with metadata.
        
        Args:
            text: Input text to chunk
            document_id: Original document ID
            chunk_method: Chunking method ("words" only supported)
            
        Returns:
            List of chunk dictionaries with metadata
        """
        chunks = list(self.chunk_by_words(text))
        
        chunked_data = []
        for idx, chunk in enumerate(chunks):
            chunk_data = {
                "id": f"{document_id}_chunk_{idx}",
                "document_id": document_id,
                "chunk_index": idx,
                "text": chunk,
                "word_count": len(chunk.split()),
                "char_count": len(chunk),
                "chunk_method": chunk_method
            }
            chunked_data.append(chunk_data)
        
        logger.info(f"Created {len(chunks)} chunks for document {document_id}")
        return chunked_data
