"""
Main RAG pipeline coordinator that orchestrates the entire process.
"""

import logging
import time
from typing import List, Dict, Any, Optional
from pathlib import Path

from config import config
from modules.pdf_extractor import PDFExtractor
from modules.cdc_handler import CDCHandler
from modules.bytewax_processor import BytewaxProcessor
from modules.qdrant_handler import QdrantHandler

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RAGPipeline:
    """
    New RAG pipeline with CDC and Bytewax processing:
    PDF → MongoDB → CDC → RabbitMQ → Bytewax → Qdrant
    """
    
    def __init__(self):
        self.pdf_extractor = None
        self.cdc_handler = None
        self.bytewax_processor = None
        self.qdrant_handler = None
        self.setup_components()
    
    def setup_components(self):
        """Initialize all pipeline components."""
        try:
            logger.info("Initializing new pipeline components...")
            
            self.pdf_extractor = PDFExtractor()
            self.cdc_handler = CDCHandler()
            self.bytewax_processor = BytewaxProcessor()
            self.qdrant_handler = QdrantHandler()
            
            logger.info("All pipeline components initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize pipeline components: {e}")
            raise
    
    def extract_pdfs_step(self, pdf_directory: Optional[str] = None) -> List[str]:
        """
        Step 1: Extract PDFs and store in MongoDB.
        
        Args:
            pdf_directory: Path to directory containing PDF files
            
        Returns:
            List of MongoDB document IDs
        """
        logger.info("=== Starting PDF Extraction Step ===")
        
        pdf_dir = pdf_directory or config.input_pdf_dir
        
        try:
            document_ids = self.pdf_extractor.process_pdf_directory(pdf_dir)
            logger.info(f"PDF extraction completed. Processed {len(document_ids)} documents")
            return document_ids
            
        except Exception as e:
            logger.error(f"PDF extraction step failed: {e}")
            raise
    
    def cdc_step(self) -> int:
        """
        Step 2: Send raw documents from MongoDB to RabbitMQ via CDC.
        
        Returns:
            Number of raw documents sent to queue
        """
        logger.info("=== Starting CDC Step ===")
        
        try:
            # Process batch and send to RabbitMQ raw queue
            documents_sent = self.cdc_handler.process_batch()
            
            logger.info(f"CDC step completed. Sent {documents_sent} raw documents to queue")
            return documents_sent
            
        except Exception as e:
            logger.error(f"CDC step failed: {e}")
            raise
    
    def bytewax_processing_step(self, run_continuous: bool = True) -> int:
        """
        Step 3: Run Bytewax processing pipeline.
        This will consume raw documents from RabbitMQ, chunk them, and store in Qdrant.
        
        Args:
            run_continuous: Whether to run continuously or in batch mode
            
        Returns:
            Status code (0 for continuous mode)
        """
        logger.info("=== Starting Bytewax Processing Step ===")
        
        try:
            if run_continuous:
                # Run Bytewax processor (this will block)
                logger.info("Starting Bytewax continuous processing...")
                self.bytewax_processor.run()
                return 0
            else:
                # For batch mode, we need to implement a different approach
                logger.info("Batch mode for Bytewax not yet implemented")
                # Could implement by checking queue size and running for limited time
                return 0
                
        except Exception as e:
            logger.error(f"Bytewax processing step failed: {e}")
            raise
    
    def run_full_pipeline(self, pdf_directory: Optional[str] = None, 
                         continuous_mode: bool = False) -> Dict[str, Any]:
        """
        Run the complete new RAG pipeline:
        PDF → MongoDB → CDC → RabbitMQ → Bytewax → Qdrant
        
        Args:
            pdf_directory: Path to directory containing PDF files
            continuous_mode: Whether to run Bytewax processing in continuous mode
            
        Returns:
            Pipeline execution summary
        """
        logger.info("🚀 Starting New RAG Pipeline with CDC and Bytewax")
        
        start_time = time.time()
        summary = {
            "start_time": start_time,
            "pdf_extraction": {"documents": 0, "success": False},
            "cdc_processing": {"documents": 0, "success": False},
            "bytewax_processing": {"status": "pending", "success": False},
            "total_time": 0,
            "success": False
        }
        
        try:
            # Step 1: PDF Extraction to MongoDB
            document_ids = self.extract_pdfs_step(pdf_directory)
            summary["pdf_extraction"]["documents"] = len(document_ids)
            summary["pdf_extraction"]["success"] = True
            
            # Step 2: CDC - Send raw documents to RabbitMQ
            documents_sent = self.cdc_step()
            summary["cdc_processing"]["documents"] = documents_sent
            summary["cdc_processing"]["success"] = True
            
            # Step 3: Bytewax Processing (Chunking + Embedding + Qdrant)
            if documents_sent > 0:
                logger.info("Starting Bytewax processing...")
                self.bytewax_processing_step(run_continuous=continuous_mode)
                summary["bytewax_processing"]["status"] = "running" if continuous_mode else "completed"
                summary["bytewax_processing"]["success"] = True
            else:
                logger.info("No documents to process with Bytewax")
                summary["bytewax_processing"]["status"] = "skipped"
                summary["bytewax_processing"]["success"] = True
            
            summary["success"] = True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            summary["error"] = str(e)
        
        finally:
            summary["total_time"] = time.time() - start_time
            logger.info(f"Pipeline completed in {summary['total_time']:.2f} seconds")
            self.cleanup()
        
        return summary
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get current status of all pipeline components.
        
        Returns:
            Status information dictionary
        """
        status = {
            "timestamp": time.time(),
            "components": {},
            "queue_info": {},
            "collection_info": {}
        }
        
        try:
            # CDC Handler status (includes RabbitMQ raw queue info)
            status["queue_info"] = self.cdc_handler.get_queue_stats()
            
            # Qdrant status
            status["collection_info"] = self.qdrant_handler.get_collection_info()
            
            # MongoDB status (through PDF extractor)
            unprocessed_docs = self.pdf_extractor.get_unprocessed_documents()
            status["components"]["mongodb"] = {
                "unprocessed_documents": len(unprocessed_docs),
                "connection": "healthy"
            }
            
            status["components"]["cdc"] = {
                "connection": "healthy"
            }
            
            status["components"]["qdrant"] = {
                "connection": "healthy"
            }
            
        except Exception as e:
            logger.error(f"Failed to get pipeline status: {e}")
            status["error"] = str(e)
        
        return status
    
    def cleanup(self):
        """Clean up pipeline resources."""
        try:
            if self.pdf_extractor:
                self.pdf_extractor.close()
            
            if self.rabbitmq_handler:
                self.rabbitmq_handler.close()
            
            logger.info("Pipeline cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


def main():
    """Main function to run the new pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description="New RAG Pipeline with CDC and Bytewax")
    parser.add_argument("--pdf-dir", help="PDF directory path", default=None)
    parser.add_argument("--continuous", action="store_true", 
                       help="Run Bytewax processing in continuous mode")
    parser.add_argument("--step", choices=["extract", "cdc", "bytewax", "full"],
                       default="full", help="Which step to run")
    
    args = parser.parse_args()
    
    pipeline = RAGPipeline()
    
    try:
        if args.step == "extract":
            result = pipeline.extract_pdfs_step(args.pdf_dir)
            logger.info(f"Extraction result: {len(result)} documents processed")
            
        elif args.step == "cdc":
            result = pipeline.cdc_step()
            logger.info(f"CDC result: {result} raw documents sent to queue")
            
        elif args.step == "bytewax":
            result = pipeline.bytewax_processing_step(run_continuous=args.continuous)
            logger.info(f"Bytewax processing started")
            
        elif args.step == "full":
            result = pipeline.run_full_pipeline(args.pdf_dir, args.continuous)
            logger.info(f"Full pipeline result: {result}")
    
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
    finally:
        pipeline.cleanup()


if __name__ == "__main__":
    main()
