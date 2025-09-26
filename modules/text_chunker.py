"""
Text chunking module for splitting text into smaller segments.
"""

import logging
from typing import List, Generator, Dict, Any

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
    
    def chunk_by_sentences(self, text: str) -> Generator[str, None, None]:
        """
        Split text into chunks by sentences, respecting word limits.
        
        Args:
            text: Input text to chunk
            
        Yields:
            Text chunks
        """
        if not text or not text.strip():
            return
        
        # Simple sentence splitting (can be improved with nltk or spacy)
        import re
        sentences = re.split(r'(?<=[.!?])\s+', text.strip())
        
        current_chunk = []
        current_word_count = 0
        
        for sentence in sentences:
            sentence_words = len(sentence.split())
            
            # If adding this sentence exceeds chunk size, yield current chunk
            if current_word_count + sentence_words > self.chunk_size and current_chunk:
                yield " ".join(current_chunk)
                
                # Start new chunk with overlap
                if self.overlap > 0:
                    overlap_words = []
                    overlap_count = 0
                    for prev_sentence in reversed(current_chunk):
                        prev_words = prev_sentence.split()
                        if overlap_count + len(prev_words) <= self.overlap:
                            overlap_words.insert(0, prev_sentence)
                            overlap_count += len(prev_words)
                        else:
                            # Take partial sentence if needed
                            remaining_overlap = self.overlap - overlap_count
                            if remaining_overlap > 0:
                                partial_sentence = " ".join(prev_words[-remaining_overlap:])
                                overlap_words.insert(0, partial_sentence)
                            break
                    
                    current_chunk = overlap_words
                    current_word_count = sum(len(s.split()) for s in overlap_words)
                else:
                    current_chunk = []
                    current_word_count = 0
            
            current_chunk.append(sentence)
            current_word_count += sentence_words
        
        # Yield the last chunk if it exists
        if current_chunk:
            yield " ".join(current_chunk)
    
    def chunk_text_with_metadata(self, text: str, document_id: str, 
                                 chunk_method: str = "words") -> List[Dict[str, Any]]:
        """
        Chunk text and return with metadata.
        
        Args:
            text: Input text to chunk
            document_id: Original document ID
            chunk_method: Chunking method ("words" or "sentences")
            
        Returns:
            List of chunk dictionaries with metadata
        """
        if chunk_method == "sentences":
            chunks = list(self.chunk_by_sentences(text))
        else:
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
    
    def get_chunk_stats(self, chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Get statistics about the chunks.
        
        Args:
            chunks: List of chunk dictionaries
            
        Returns:
            Statistics dictionary
        """
        if not chunks:
            return {"total_chunks": 0}
        
        word_counts = [chunk["word_count"] for chunk in chunks]
        char_counts = [chunk["char_count"] for chunk in chunks]
        
        return {
            "total_chunks": len(chunks),
            "avg_word_count": sum(word_counts) / len(word_counts),
            "min_word_count": min(word_counts),
            "max_word_count": max(word_counts),
            "avg_char_count": sum(char_counts) / len(char_counts),
            "min_char_count": min(char_counts),
            "max_char_count": max(char_counts),
            "total_words": sum(word_counts),
            "total_chars": sum(char_counts)
        }
