import pytest
import os
import json
import zlib
import tempfile
from atomicdb.storage import JSONStorage, CompressedJSONStorage

@pytest.fixture
def test_data():
    """Test data for storage tests."""
    return [
        {"id": 1, "name": "test1"},
        {"id": 2, "name": "test2"}
    ]

@pytest.fixture
def json_file():
    """Create a temporary JSON file."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        yield f.name
    os.unlink(f.name)

@pytest.fixture
def json_storage(json_file):
    """Create a JSON storage instance."""
    storage = JSONStorage(json_file)
    yield storage
    storage.close()

@pytest.fixture
def temp_file(tmp_path):
    """Create a temporary file for testing."""
    return str(tmp_path / "test.json")

def test_json_storage(json_storage, test_data):
    """Test JSON storage backend."""
    # Test save
    json_storage.save(test_data)
    
    # Test load
    loaded_data = json_storage.load()
    assert loaded_data == test_data

def test_json_storage_no_compression(temp_file):
    """Test JSON storage without compression."""
    storage = JSONStorage(temp_file, compression_level=0)
    data = [{"name": "test", "value": 123}]
    
    # Save data
    storage.save(data)
    assert os.path.exists(temp_file)
    
    # Verify it's stored as plain JSON
    with open(temp_file, 'r') as f:
        stored_data = json.load(f)
    assert stored_data == data
    
    # Load data
    loaded_data = storage.load()
    assert loaded_data == data

def test_json_storage_with_compression(temp_file):
    """Test JSON storage with compression."""
    storage = JSONStorage(temp_file, compression_level=6)
    data = [{"name": "test", "value": 123}]
    
    # Save data
    storage.save(data)
    assert os.path.exists(temp_file)
    
    # Verify it's stored as compressed data
    with open(temp_file, 'rb') as f:
        stored_data = f.read()
    decompressed = zlib.decompress(stored_data)
    assert json.loads(decompressed) == data
    
    # Load data
    loaded_data = storage.load()
    assert loaded_data == data

def test_compressed_json_storage(temp_file):
    """Test CompressedJSONStorage class."""
    storage = CompressedJSONStorage(temp_file)
    data = [{"name": "test", "value": 123}]
    
    # Save data
    storage.save(data)
    assert os.path.exists(temp_file)
    
    # Verify it's stored as compressed data
    with open(temp_file, 'rb') as f:
        stored_data = f.read()
    decompressed = zlib.decompress(stored_data)
    assert json.loads(decompressed) == data
    
    # Load data
    loaded_data = storage.load()
    assert loaded_data == data

def test_compression_level_validation():
    """Test compression level validation."""
    with pytest.raises(ValueError):
        CompressedJSONStorage("test.json", compression_level=0)
    
    with pytest.raises(ValueError):
        CompressedJSONStorage("test.json", compression_level=-1)

def test_backward_compatibility(temp_file):
    """Test loading uncompressed files with compression enabled."""
    # First save uncompressed
    uncompressed = JSONStorage(temp_file, compression_level=0)
    data = [{"name": "test", "value": 123}]
    uncompressed.save(data)
    
    # Then load with compression enabled
    compressed = JSONStorage(temp_file, compression_level=6)
    loaded_data = compressed.load()
    assert loaded_data == data

def test_large_dataset_compression(temp_file):
    """Test compression with a larger dataset."""
    storage = CompressedJSONStorage(temp_file)
    
    # Create a larger dataset
    data = [
        {
            "id": i,
            "name": f"test_{i}",
            "description": "A" * 1000,  # 1KB of text
            "values": list(range(100))
        }
        for i in range(100)  # 100 documents
    ]
    
    # Save data and get uncompressed size
    uncompressed_size = len(json.dumps(data).encode())
    storage.save(data)
    
    # Get compressed size
    compressed_size = os.path.getsize(temp_file)
    
    # Verify compression ratio
    compression_ratio = compressed_size / uncompressed_size
    assert compression_ratio < 0.5  # Should achieve at least 50% compression
    
    # Verify data integrity
    loaded_data = storage.load()
    assert loaded_data == data
