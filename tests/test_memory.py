import pytest
import os
from pathlib import Path
from atomicdb import AtomicDB
from atomicdb.storage import MemoryStorage

@pytest.fixture
def db():
    """Create a temporary in-memory database."""
    return AtomicDB(in_memory=True)

def test_in_memory_storage(db):
    """Test basic in-memory storage operations."""
    # Insert data
    db.insert({"name": "test"})
    assert len(db.find()) == 1
    
    # Data should persist in memory
    result = db.find_one({"name": "test"})
    assert result["name"] == "test"
    
    # Close and reopen should start fresh
    db.close()
    new_db = AtomicDB(in_memory=True)
    assert len(new_db.find()) == 0

def test_memory_with_path(tmp_path):
    """Test in-memory storage with disk persistence."""
    db_file = tmp_path / "test.json"
    
    # Create database and add data
    db = AtomicDB(path=db_file, in_memory=True)
    db.insert({"name": "test"})
    
    # Data should be in memory but not on disk yet
    assert len(db.find()) == 1
    assert not db_file.exists()
    
    # Commit should write to disk
    db.commit()
    assert db_file.exists()
    
    # New connection should start fresh but can load from disk
    db.close()
    new_db = AtomicDB(path=db_file, in_memory=True)
    assert len(new_db.find()) == 1
    result = new_db.find_one({"name": "test"})
    assert result["name"] == "test"

def test_memory_clear(tmp_path):
    """Test clearing in-memory database."""
    db_file = tmp_path / "test.json"
    db = AtomicDB(path=db_file, in_memory=True)
    
    # Add and commit data
    db.insert({"name": "test"})
    db.commit()
    assert db_file.exists()
    
    # Clear should remove from both memory and disk
    db.clear()
    assert len(db.find()) == 0
    assert not db_file.exists()

def test_memory_to_disk_conversion(tmp_path):
    """Test converting in-memory database to disk storage."""
    # Start with in-memory
    db = AtomicDB(in_memory=True)
    db.insert({"name": "test"})
    
    # Create new disk-based database
    db_file = tmp_path / "test.json"
    disk_db = AtomicDB(path=db_file)
    
    # Copy data from memory to disk
    for doc in db.find():
        disk_db.insert(doc)
    
    # Verify data persisted
    assert len(disk_db.find()) == 1
    result = disk_db.find_one({"name": "test"})
    assert result["name"] == "test"
