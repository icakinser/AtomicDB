import pytest
import threading
import time
import queue
from atomicdb.pool import ThreadSafeDatabase, ConnectionPool

@pytest.fixture
def db(tmp_path):
    """Create a temporary thread-safe database for testing."""
    db_file = tmp_path / "test.json"
    return ThreadSafeDatabase(str(db_file))

def test_basic_operations(db):
    """Test basic operations with thread safety."""
    # Insert
    doc_id = db.insert({"name": "Alice", "age": 30})
    assert doc_id == 0
    
    # Get
    result = db.get(lambda x: x["name"] == "Alice")
    assert result.first()["age"] == 30
    
    # Update
    db.update({"age": 31}, lambda x: x["name"] == "Alice")
    result = db.get(lambda x: x["name"] == "Alice")
    assert result.first()["age"] == 31
    
    # Remove
    db.remove(lambda x: x["name"] == "Alice")
    result = db.get(lambda x: x["name"] == "Alice")
    assert result.is_empty()

def test_concurrent_inserts(db):
    """Test concurrent insert operations."""
    num_threads = 3
    docs_per_thread = 20
    
    def insert_docs():
        for i in range(docs_per_thread):
            db.insert({"thread": threading.current_thread().name, "count": i})
    
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=insert_docs, name=f"Thread-{i}")
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    # Verify all documents were inserted
    results = db.search(lambda x: True)
    assert results.count() == num_threads * docs_per_thread

def test_concurrent_reads(db):
    """Test concurrent read operations."""
    # Insert test data
    for i in range(20):
        db.insert({"id": i, "value": i * 2})
    
    results = []
    lock = threading.Lock()
    
    def read_docs():
        thread_results = []
        for i in range(5):
            result = db.get(lambda x: x["id"] == i)
            if not result.is_empty():
                thread_results.append(result.first())
        with lock:
            results.extend(thread_results)
    
    threads = []
    for i in range(3):
        t = threading.Thread(target=read_docs)
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    # Verify reads were successful
    assert len(results) > 0

def test_concurrent_updates(db):
    """Test concurrent update operations."""
    # Insert test document
    db.insert({"counter": 0})
    
    def increment_counter():
        for _ in range(20):
            def update_op(doc):
                return {"counter": doc["counter"] + 1}
            db.atomic_update(update_op)
    
    threads = []
    for _ in range(3):
        t = threading.Thread(target=increment_counter)
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    # Verify final counter value
    result = db.get(lambda x: True)
    assert result.first()["counter"] == 60

def test_connection_pool_limits(tmp_path):
    """Test connection pool limits."""
    db_file = tmp_path / "test.json"
    pool = ConnectionPool(str(db_file), max_connections=2)
    
    # Get two connections (should work)
    conn1 = pool.get_connection()
    conn2 = pool.get_connection()
    
    # Try to get third connection (should raise error)
    with pytest.raises(queue.Empty):
        pool.get_connection()
    
    # Return connection and try again (should work)
    pool.return_connection(conn1)
    conn3 = pool.get_connection()
    
    # Clean up
    pool.return_connection(conn2)
    pool.return_connection(conn3)
    pool.close_all()

def test_connection_timeout(tmp_path):
    """Test connection timeout."""
    db_file = tmp_path / "test.json"
    pool = ConnectionPool(str(db_file), max_connections=1, timeout=0.1)
    
    # Get the only connection
    conn = pool.get_connection()
    
    # Try to get another connection (should timeout)
    with pytest.raises(queue.Empty):
        pool.get_connection()
    
    # Return connection and try again (should work)
    pool.return_connection(conn)
    conn2 = pool.get_connection()
    
    # Clean up
    pool.return_connection(conn2)
    pool.close_all()

def test_context_manager(tmp_path):
    """Test database context manager."""
    db_file = tmp_path / "test.json"
    
    with ThreadSafeDatabase(str(db_file)) as db:
        db.insert({"test": "data"})
        result = db.get(lambda x: x["test"] == "data")
        assert not result.is_empty()
    
    # Verify we can create new connection after context
    with ThreadSafeDatabase(str(db_file)) as db:
        result = db.get(lambda x: x["test"] == "data")
        assert not result.is_empty()
