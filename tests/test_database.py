import pytest
from atomicdb import AtomicDB
from atomicdb.storage import JSONStorage
import os
import tempfile

@pytest.fixture
def db(tmp_path):
    """Create a temporary database for testing."""
    db_file = tmp_path / "test.json"
    db = AtomicDB(storage=JSONStorage(str(db_file)))
    yield db
    if os.path.exists(str(db_file)):
        os.remove(str(db_file))

def test_insert_and_get(db):
    """Test inserting and retrieving documents."""
    doc = {"name": "John", "age": 30}
    idx = db.insert(doc)
    assert idx == 0

    # Test with lambda
    result = db.get(lambda x: x["name"] == "John")
    assert result.first() == doc
    
    # Test with query
    query = db.query().name == "John"
    result = db.get(query)
    assert result.first() == doc

def test_insert_many(db):
    """Test inserting multiple documents."""
    docs = [
        {"name": "John", "age": 30},
        {"name": "Jane", "age": 25}
    ]
    ids = db.insert_many(docs)
    assert ids == [0, 1]
    assert len(db.all()) == 2

def test_update(db):
    """Test updating documents."""
    db.insert({"name": "John", "age": 30})
    db.insert({"name": "Jane", "age": 25})

    # Test with lambda
    count = db.update({"age": 31}, lambda x: x["name"] == "John")
    assert count == 1
    result = db.get(lambda x: x["name"] == "John")
    assert result.first()["age"] == 31

    # Test with query
    query = db.query().name == "Jane"
    count = db.update({"age": 26}, query)
    assert count == 1
    result = db.get(query)
    assert result.first()["age"] == 26

def test_remove(db):
    """Test removing documents."""
    db.insert({"name": "John", "age": 30})
    db.insert({"name": "Jane", "age": 25})

    # Test with lambda
    count = db.remove(lambda x: x["name"] == "John")
    assert count == 1
    assert len(db.all()) == 1
    assert db.get(lambda x: x["name"] == "John").is_empty()

    # Test with query
    query = db.query().name == "Jane"
    count = db.remove(query)
    assert count == 1
    assert len(db.all()) == 0

def test_persistence(db):
    """Test that data persists between database instances."""
    db.insert({"name": "John", "age": 30})

    # Create new instance with same file
    db2 = AtomicDB(storage=JSONStorage(db.path))
    assert len(db2.all()) == 1
    query = db2.query().name == "John"
    result = db2.get(query)
    assert result.first()["age"] == 30

def test_query_operators(db):
    """Test various query operators."""
    doc = {
        "name": "John Smith",
        "age": 30,
        "email": "john@example.com",
        "tags": ["developer", "python"]
    }
    db.insert(doc)

    # Test regex matching
    query = db.query().name.matches(r"John.*")
    result = db.get(query)
    assert result.first() == doc

    # Test greater than
    query = db.query().age > 25
    result = db.get(query)
    assert result.first() == doc

    # Test less than
    query = db.query().age < 35
    result = db.get(query)
    assert result.first() == doc

    # Test contains
    query = db.query().tags.contains("python")
    result = db.get(query)
    assert result.first() == doc

def test_complex_queries(db):
    """Test complex query combinations."""
    doc = {
        "name": "John Smith",
        "age": 30,
        "email": "john@example.com",
        "active": True
    }
    db.insert(doc)

    # Test AND
    query = (db.query().name == "John Smith") & (db.query().age == 30)
    result = db.get(query)
    assert result.first() == doc

    # Test OR
    query = (db.query().name == "John Smith") | (db.query().name == "Jane Doe")
    result = db.get(query)
    assert result.first() == doc

    # Test NOT
    query = ~(db.query().name == "Jane Doe")
    result = db.get(query)
    assert result.first() == doc

def test_clear(db):
    """Test clearing all documents."""
    db.insert({"name": "John", "age": 30})
    db.insert({"name": "Jane", "age": 25})

    db.clear()
    assert len(db.all()) == 0
    assert db.all().is_empty()

def test_empty_database(db):
    """Test operations on empty database."""
    assert len(db.all()) == 0
    assert db.get(db.query().name == "John").is_empty()
    assert db.search(db.query().name == "John").is_empty()
