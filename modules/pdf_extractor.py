"""
PDF extraction module for converting PDFs to markdown and storing in MongoDB.
"""

import os
import datetime
import logging
from typing import List, Dict, Any
from docling.document_converter import DocumentConverter
from pymongo import MongoClient

from config import config

logger = logging.getLogger(__name__)

class PDFExtractor:
    """Handles PDF extraction and MongoDB storage."""
    
    def __init__(self):
        self.converter = DocumentConverter()
        self.mongo_client = MongoClient(config.mongo.uri)
        self.db = self.mongo_client[config.mongo.database]
        self.collection = self.db[config.mongo.collection]
    
    def extract_pdf_to_markdown(self, pdf_path: str) -> str:
        """Extract text from PDF and convert to markdown."""
        try:
            result = self.converter.convert(pdf_path)
            doc = result.document
            return doc.export_to_markdown()
        except Exception as e:
            logger.error(f"Error extracting PDF {pdf_path}: {e}")
            raise
    
    def store_markdown_to_mongo(self, pdf_path: str, markdown_content: str) -> str:
        """Store markdown content to MongoDB."""
        try:
            record = {
                "file_name": os.path.basename(pdf_path),
                "file_path": pdf_path,
                "markdown": markdown_content,
                "created_at": datetime.datetime.utcnow(),
                "processed": False  # Flag for CDC processing
            }
            
            result = self.collection.insert_one(record)
            logger.info(f"Stored {pdf_path} in MongoDB with ID: {result.inserted_id}")
            return str(result.inserted_id)
            
        except Exception as e:
            logger.error(f"Error storing to MongoDB: {e}")
            raise
    
    def process_pdf_directory(self, directory_path: str) -> List[str]:
        """Process all PDF files in a directory."""
        if not os.path.exists(directory_path):
            raise FileNotFoundError(f"Directory not found: {directory_path}")
        
        pdf_files = [f for f in os.listdir(directory_path) if f.lower().endswith('.pdf')]
        
        if not pdf_files:
            logger.warning(f"No PDF files found in directory: {directory_path}")
            return []
        
        logger.info(f"Found {len(pdf_files)} PDF files to process")
        
        processed_ids = []
        failed_files = []
        
        for pdf_file in pdf_files:
            pdf_path = os.path.join(directory_path, pdf_file)
            try:
                # Extract and store
                logger.info(f"Processing PDF: {pdf_path}")
                markdown_content = self.extract_pdf_to_markdown(pdf_path)
                doc_id = self.store_markdown_to_mongo(pdf_path, markdown_content)
                processed_ids.append(doc_id)
                logger.info(f"Successfully processed PDF: {pdf_path}")
            except Exception as e:
                logger.error(f"Failed to process {pdf_path}: {e}")
                failed_files.append(pdf_path)
        
        logger.info(f"Successfully processed {len(processed_ids)} files")
        if failed_files:
            logger.warning(f"Failed to process {len(failed_files)} files: {failed_files}")
        
        return processed_ids
    
    def get_unprocessed_documents(self) -> List[Dict[str, Any]]:
        """Get documents from MongoDB that haven't been processed yet."""
        try:
            cursor = self.collection.find({"processed": False})
            return list(cursor)
        except Exception as e:
            logger.error(f"Error fetching unprocessed documents: {e}")
            raise
    
    def close(self):
        """Close MongoDB connection."""
        self.mongo_client.close()
