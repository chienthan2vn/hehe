"""
Test script for the new RAG pipeline with CDC and Bytewax processing.
"""

import logging
import time
import sys
from pathlib import Path

# Add current directory to path for imports
sys.path.append(str(Path(__file__).parent))

from config import config
from modules.pdf_extractor import PDFExtractor
from modules.cdc_handler import CDCHandler
from modules.bytewax_processor import BytewaxProcessor
from modules.qdrant_handler import QdrantHandler
from pipeline import RAGPipeline

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestNewPipeline:
    """Test suite for the new RAG pipeline."""
    
    def __init__(self):
        self.test_results = {
            "config_test": False,
            "pdf_extractor_test": False,
            "cdc_handler_test": False,
            "bytewax_processor_test": False,
            "qdrant_handler_test": False,
            "pipeline_integration_test": False
        }
    
    def test_config_loading(self) -> bool:
        """Test configuration loading."""
        logger.info("Testing configuration loading...")
        
        try:
            # Test all config sections
            assert hasattr(config, 'mongo'), "MongoDB config missing"
            assert hasattr(config, 'rabbitmq'), "RabbitMQ config missing"
            assert hasattr(config, 'qdrant'), "Qdrant config missing"
            assert hasattr(config, 'chunking'), "Chunking config missing"
            assert hasattr(config, 'embedding'), "Embedding config missing"
            
            # Test MongoDB config
            assert config.mongo.uri, "MongoDB URI not set"
            assert config.mongo.database, "MongoDB database not set"
            assert config.mongo.collection, "MongoDB collection not set"
            
            # Test RabbitMQ config
            assert config.rabbitmq.host, "RabbitMQ host not set"
            assert config.rabbitmq.queue, "RabbitMQ queue not set"
            
            # Test Qdrant config
            assert config.qdrant.host, "Qdrant host not set"
            assert config.qdrant.collection_name, "Qdrant collection not set"
            
            logger.info("✅ Configuration loading test passed")
            return True
            
        except AssertionError as e:
            logger.error(f"❌ Configuration loading test failed: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Configuration loading test error: {e}")
            return False
    
    def test_pdf_extractor(self) -> bool:
        """Test PDF Extractor functionality."""
        logger.info("Testing PDF Extractor...")
        
        try:
            pdf_extractor = PDFExtractor()
            
            # Test connection
            unprocessed = pdf_extractor.get_unprocessed_documents()
            logger.info(f"Found {len(unprocessed)} unprocessed documents")
            
            pdf_extractor.close()
            logger.info("✅ PDF Extractor test passed")
            return True
            
        except Exception as e:
            logger.error(f"❌ PDF Extractor test failed: {e}")
            return False
    
    def test_cdc_handler(self) -> bool:
        """Test CDC Handler functionality."""
        logger.info("Testing CDC Handler...")
        
        try:
            with CDCHandler() as cdc_handler:
                # Test queue setup
                queue_stats = cdc_handler.get_queue_stats()
                logger.info(f"Raw queue stats: {queue_stats}")
                
                # Test purge functionality (for clean testing)
                purged_count = cdc_handler.purge_raw_queue()
                logger.info(f"Purged {purged_count} messages from raw queue")
                
            logger.info("✅ CDC Handler test passed")
            return True
            
        except Exception as e:
            logger.error(f"❌ CDC Handler test failed: {e}")
            return False
    
    def test_bytewax_processor(self) -> bool:
        """Test Bytewax Processor initialization."""
        logger.info("Testing Bytewax Processor...")
        
        try:
            # Test creating dataflow function directly first
            from modules.bytewax_processor import create_rag_processing_flow
            flow = create_rag_processing_flow()
            
            # Check if flow is properly created
            assert flow is not None, "Bytewax flow not created"
            
            # Check flow type instead of name
            from bytewax.dataflow import Dataflow
            assert isinstance(flow, Dataflow), "Flow is not a Dataflow object"
            
            # Test BytewaxProcessor initialization
            bytewax_processor = BytewaxProcessor()
            assert bytewax_processor.flow is not None, "BytewaxProcessor flow not created"
            assert isinstance(bytewax_processor.flow, Dataflow), "BytewaxProcessor flow is not a Dataflow object"
            
            logger.info("✅ Bytewax Processor test passed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Bytewax Processor test failed: {e}")
            logger.error(f"Error details: {str(e)}")
            return False
    
    def test_qdrant_handler(self) -> bool:
        """Test Qdrant Handler functionality."""
        logger.info("Testing Qdrant Handler...")
        
        try:
            qdrant_handler = QdrantHandler()
            
            # Test connection and collection info
            collection_info = qdrant_handler.get_collection_info()
            logger.info(f"Qdrant collection info: {collection_info}")
            
            logger.info("✅ Qdrant Handler test passed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Qdrant Handler test failed: {e}")
            return False
    
    def test_pipeline_integration(self) -> bool:
        """Test pipeline integration without full execution."""
        logger.info("Testing Pipeline Integration...")
        
        try:
            pipeline = RAGPipeline()
            
            # Test component initialization
            assert pipeline.pdf_extractor is not None, "PDF Extractor not initialized"
            assert pipeline.cdc_handler is not None, "CDC Handler not initialized"
            assert pipeline.bytewax_processor is not None, "Bytewax Processor not initialized"
            assert pipeline.qdrant_handler is not None, "Qdrant Handler not initialized"
            
            # Test status retrieval
            status = pipeline.get_pipeline_status()
            logger.info(f"Pipeline status: {status}")
            
            pipeline.cleanup()
            logger.info("✅ Pipeline Integration test passed")
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline Integration test failed: {e}")
            return False
    
    def run_all_tests(self) -> bool:
        """Run all tests."""
        logger.info("🧪 Starting New RAG Pipeline Test Suite")
        logger.info("=" * 50)
        
        # Run tests
        tests = [
            ("config_test", self.test_config_loading),
            ("pdf_extractor_test", self.test_pdf_extractor),
            ("cdc_handler_test", self.test_cdc_handler),
            ("bytewax_processor_test", self.test_bytewax_processor),
            ("qdrant_handler_test", self.test_qdrant_handler),
            ("pipeline_integration_test", self.test_pipeline_integration),
        ]
        
        for test_name, test_func in tests:
            logger.info(f"\n--- Running {test_name} ---")
            try:
                self.test_results[test_name] = test_func()
                time.sleep(1)  # Brief pause between tests
            except Exception as e:
                logger.error(f"Test {test_name} crashed: {e}")
                self.test_results[test_name] = False
        
        # Summary
        logger.info("\n" + "=" * 50)
        logger.info("📊 TEST RESULTS SUMMARY")
        logger.info("=" * 50)
        
        passed_tests = 0
        total_tests = len(self.test_results)
        
        for test_name, result in self.test_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            logger.info(f"{test_name:<25}: {status}")
            if result:
                passed_tests += 1
        
        logger.info("-" * 50)
        logger.info(f"Total: {passed_tests}/{total_tests} tests passed")
        
        success_rate = (passed_tests / total_tests) * 100
        logger.info(f"Success Rate: {success_rate:.1f}%")
        
        if passed_tests == total_tests:
            logger.info("🎉 All tests passed! New pipeline is ready to use.")
            return True
        else:
            logger.warning("⚠️  Some tests failed. Please check the components.")
            return False

def test_step_by_step():
    """Test pipeline steps individually."""
    logger.info("🔍 Testing Pipeline Steps Individually")
    logger.info("=" * 50)
    
    try:
        pipeline = RAGPipeline()
        
        # Test Step 1: PDF Extraction (if PDFs available)
        logger.info("\n--- Testing Step 1: PDF Extraction ---")
        try:
            # Check if there are PDFs to process
            pdf_dir = getattr(config, 'input_pdf_dir', 'src_data')
            if Path(pdf_dir).exists():
                documents = pipeline.extract_pdfs_step(pdf_dir)
                logger.info(f"✅ PDF extraction completed: {len(documents)} documents")
            else:
                logger.info("ℹ️  No PDF directory found, skipping extraction test")
        except Exception as e:
            logger.error(f"❌ PDF extraction failed: {e}")
        
        # Test Step 2: CDC Processing
        logger.info("\n--- Testing Step 2: CDC Processing ---")
        try:
            documents_sent = pipeline.cdc_step()
            logger.info(f"✅ CDC processing completed: {documents_sent} documents sent")
        except Exception as e:
            logger.error(f"❌ CDC processing failed: {e}")
        
        # Test Step 3: Bytewax Processor (initialization only)
        logger.info("\n--- Testing Step 3: Bytewax Processor (init only) ---")
        try:
            # Don't run full processing, just test initialization
            processor = pipeline.bytewax_processor
            logger.info("✅ Bytewax processor initialized successfully")
            logger.info("ℹ️  Full Bytewax processing test skipped (would run continuously)")
        except Exception as e:
            logger.error(f"❌ Bytewax processor initialization failed: {e}")
        
        pipeline.cleanup()
        logger.info("\n✅ Step-by-step testing completed")
        
    except Exception as e:
        logger.error(f"❌ Step-by-step testing failed: {e}")

def main():
    """Main test function."""
    test_suite = TestNewPipeline()
    
    # Run basic tests
    all_passed = test_suite.run_all_tests()
    
    if all_passed:
        # If basic tests pass, run step-by-step tests
        logger.info("\n" + "=" * 50)
        test_step_by_step()
    
    logger.info("\n🏁 Testing completed!")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
