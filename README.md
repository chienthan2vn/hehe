# RAG Paper Pipeline

Một pipeline hoàn chỉnh để xử lý PDF documents thông qua RAG (Retrieval-Augmented Generation) architecture với MongoDB, RabbitMQ, và Qdrant.

## 📋 Tổng quan Pipeline

Pipeline này thực hiện các bước sau:

1. **PDF Extraction**: Sử dụng Docling để extract PDF thành markdown và lưu vào MongoDB
2. **Text Chunking**: Chia text thành các chunks nhỏ hơn với metadata
3. **Message Queue**: Sử dụng RabbitMQ để queue các chunks để xử lý
4. **Vector Storage**: Tạo embeddings và lưu vào Qdrant vector database

## 🏗️ Kiến trúc

```
PDF Files → Docling → MongoDB → RabbitMQ → Embedding → Qdrant
     ↓         ↓         ↓          ↓          ↓         ↓
  Extract   Markdown   Store    Queue     Generate   Vector
             Text               Chunks   Embeddings   Search
```

## 📁 Cấu trúc Project

```
rag_paper/
├── config.py                      # Configuration settings
├── pipeline.py                    # Main pipeline coordinator
├── test_refactored_pipeline.py   # Comprehensive tests
├── requirements.txt               # Dependencies
├── modules/
│   ├── __init__.py
│   ├── pdf_extractor.py         # PDF extraction & MongoDB
│   ├── text_chunker.py          # Text chunking logic
│   ├── rabbitmq_handler.py      # RabbitMQ operations
│   └── qdrant_handler.py        # Qdrant vector operations
└── src/                         # Legacy code (for reference)
```

## 🚀 Cài đặt

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Setup Services

Đảm bảo các services sau đang chạy:

- **MongoDB**: `mongodb://localhost:27001`
- **RabbitMQ**: `localhost:5672`
- **Qdrant**: `localhost:6333`

### 3. Configuration

Chỉnh sửa `config.py` nếu cần thiết:

```python
@dataclass
class MongoConfig:
    uri: str = "mongodb://localhost:27001"
    database: str = "pdf_storage"
    collection: str = "pdf_markdown"

@dataclass
class RabbitMQConfig:
    host: str = "localhost"
    port: int = 5672
    queue: str = "chunked_data"

@dataclass
class QdrantConfig:
    host: str = "localhost"
    port: int = 6333
    collection_name: str = "chunked_vectors"
```

## 💻 Sử dụng

### 1. Chạy Full Pipeline

```bash
# Chạy toàn bộ pipeline
python pipeline.py --step full

# Chỉ định thư mục PDF
python pipeline.py --step full --pdf-dir "path/to/pdf/directory"

# Chạy với continuous mode cho vector storage
python pipeline.py --step full --continuous
```

### 2. Chạy từng bước riêng lẻ

```bash
# Chỉ extract PDF
python pipeline.py --step extract --pdf-dir "src_data"

# Chỉ chunking và queue
python pipeline.py --step chunk

# Chỉ vector storage
python pipeline.py --step vector
```

### 3. Sử dụng trong code

```python
from pipeline import RAGPipeline

# Khởi tạo pipeline
pipeline = RAGPipeline()

# Chạy full pipeline
result = pipeline.run_full_pipeline(
    pdf_directory="src_data",
    continuous_mode=False
)

print(f"Pipeline result: {result}")

# Cleanup
pipeline.cleanup()
```

## 🧪 Testing

### Chạy test suite

```bash
python test_refactored_pipeline.py
```

Test suite kiểm tra:
- ✅ Configuration loading
- ✅ PDF Extractor functionality
- ✅ Text Chunker operations
- ✅ RabbitMQ Handler connections
- ✅ Qdrant Handler operations
- ✅ Pipeline Components integration
- ✅ Individual Pipeline Steps

## 📖 API Documentation

### RAGPipeline Class

#### Methods

- `extract_pdfs_step(pdf_directory=None)` - Extract PDFs và lưu vào MongoDB
- `chunking_and_queue_step()` - Chunk text và gửi vào RabbitMQ queue
- `vector_storage_step(run_consumer=True)` - Process queue và lưu vào Qdrant
- `run_full_pipeline(pdf_directory=None, continuous_mode=False)` - Chạy toàn bộ pipeline
- `get_pipeline_status()` - Lấy status của tất cả components
- `cleanup()` - Clean up resources

### Configuration Options

#### ChunkingConfig
- `chunk_size`: Số words per chunk (default: 512)
- `overlap`: Số words overlap giữa chunks (default: 50)

#### EmbeddingConfig
- `model_name`: Sentence transformer model (default: "sentence-transformers/all-MiniLM-L6-v2")

## 🔍 Monitoring

### Check Pipeline Status

```python
from pipeline import RAGPipeline

pipeline = RAGPipeline()
status = pipeline.get_pipeline_status()
print(status)
```

### Monitor Queue

```python
from modules.rabbitmq_handler import RabbitMQHandler

with RabbitMQHandler() as rabbitmq:
    queue_info = rabbitmq.get_queue_info()
    print(f"Messages in queue: {queue_info['message_count']}")
```

### Monitor Qdrant Collection

```python
from modules.qdrant_handler import QdrantHandler

qdrant = QdrantHandler()
collection_info = qdrant.get_collection_info()
print(f"Vectors in collection: {collection_info['vectors_count']}")
```

## 🔧 Troubleshooting

### Common Issues

1. **MongoDB Connection Error**
   ```bash
   # Check MongoDB is running
   mongo --host localhost:27001 --eval "db.adminCommand('ping')"
   ```

2. **RabbitMQ Connection Error**
   ```bash
   # Check RabbitMQ is running
   rabbitmqctl status
   ```

3. **Qdrant Connection Error**
   ```bash
   # Check Qdrant is running
   curl http://localhost:6333/collections
   ```

4. **Import Errors**
   ```bash
   # Make sure all dependencies are installed
   pip install -r requirements.txt
   ```

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 📈 Performance Tips

1. **Batch Processing**: Sử dụng batch mode cho vector storage khi có nhiều chunks
2. **Chunk Size**: Điều chỉnh chunk size dựa trên loại documents
3. **Embedding Model**: Có thể thay đổi model trong config để cải thiện accuracy
4. **Queue Management**: Monitor queue để tránh memory issues

## 🔄 Workflow Examples

### Basic Workflow
```python
pipeline = RAGPipeline()

# 1. Extract PDFs
doc_ids = pipeline.extract_pdfs_step("my_pdfs/")

# 2. Process chunks
chunks_sent = pipeline.chunking_and_queue_step()

# 3. Store vectors
chunks_processed = pipeline.vector_storage_step(run_consumer=False)

pipeline.cleanup()
```

### Search Workflow
```python
from modules.qdrant_handler import QdrantHandler

qdrant = QdrantHandler()
results = qdrant.search_similar(
    query="machine learning algorithms",
    limit=10,
    score_threshold=0.7
)

for result in results:
    print(f"Score: {result['score']}")
    print(f"Text: {result['text'][:200]}...")
```

## 🤝 So sánh với code cũ

### Trước khi refactor:
- ❌ Code lộn xộn trong các file riêng lẻ
- ❌ Không có structure rõ ràng
- ❌ Khó maintain và extend
- ❌ Không có error handling tốt
- ❌ Không có testing

### Sau khi refactor:
- ✅ Code được tổ chức thành modules rõ ràng
- ✅ Configuration centralized
- ✅ Error handling và logging đầy đủ
- ✅ Easy to test và maintain
- ✅ Có thể chạy từng step riêng lẻ
- ✅ Comprehensive test suite
- ✅ Documentation đầy đủ

## 📄 License

MIT License - xem file LICENSE để biết thêm chi tiết.
