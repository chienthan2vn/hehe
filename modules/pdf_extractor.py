"""
PDF extraction module for converting PDFs to markdown and storing in MongoDB.
"""

import os
import datetime
import logging
from typing import List, Dict, Any
from docling.document_converter import DocumentConverter
from pymongo import MongoClient
from pymongo.collection import Collection

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
        """
        Extract text from PDF and convert to markdown.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Extracted markdown content
        """
        try:
            result = self.converter.convert(pdf_path)
            doc = result.document
            return doc.export_to_markdown()
        except Exception as e:
            logger.error(f"Error extracting PDF {pdf_path}: {e}")
            raise
    
    def store_markdown_to_mongo(self, pdf_path: str, markdown_content: str) -> str:
        """
        Store markdown content to MongoDB.
        
        Args:
            pdf_path: Original PDF file path
            markdown_content: Extracted markdown content
            
        Returns:
            MongoDB document ID
        """
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
    
    def process_single_pdf(self, pdf_path: str) -> str:
        """
        Process a single PDF file: extract and store.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            MongoDB document ID
        """
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
        if not pdf_path.lower().endswith('.pdf'):
            raise ValueError(f"File is not a PDF: {pdf_path}")
        
        logger.info(f"Processing PDF: {pdf_path}")
        
        # Extract markdown
        markdown_content = self.extract_pdf_to_markdown(pdf_path)
        
        # Store to MongoDB
        doc_id = self.store_markdown_to_mongo(pdf_path, markdown_content)
        
        logger.info(f"Successfully processed PDF: {pdf_path}")
        return doc_id
    
    def process_pdf_directory(self, directory_path: str) -> List[str]:
        """
        Process all PDF files in a directory.
        
        Args:
            directory_path: Path to directory containing PDF files
            
        Returns:
            List of MongoDB document IDs
        """
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
                doc_id = self.process_single_pdf(pdf_path)
                processed_ids.append(doc_id)
            except Exception as e:
                logger.error(f"Failed to process {pdf_path}: {e}")
                failed_files.append(pdf_path)
        
        logger.info(f"Successfully processed {len(processed_ids)} files")
        if failed_files:
            logger.warning(f"Failed to process {len(failed_files)} files: {failed_files}")
        
        return processed_ids
    
    def get_unprocessed_documents(self) -> List[Dict[str, Any]]:
        """
        Get documents from MongoDB that haven't been processed yet.
        
        Returns:
            List of unprocessed documents
        """
        try:
            cursor = self.collection.find({"processed": False})
            return list(cursor)
        except Exception as e:
            logger.error(f"Error fetching unprocessed documents: {e}")
            raise
    
    def mark_document_processed(self, doc_id: str):
        """
        Mark a document as processed.
        
        Args:
            doc_id: MongoDB document ID
        """
        try:
            from bson import ObjectId
            self.collection.update_one(
                {"_id": ObjectId(doc_id)},
                {"$set": {"processed": True, "processed_at": datetime.datetime.utcnow()}}
            )
            logger.info(f"Marked document {doc_id} as processed")
        except Exception as e:
            logger.error(f"Error marking document as processed: {e}")
            raise
    
    def close(self):
        """Close MongoDB connection."""
        self.mongo_client.close()
