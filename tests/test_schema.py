import pytest
from atomicdb import AtomicDB
from atomicdb.schema import Schema, ValidationError

@pytest.fixture
def db():
    """Create a temporary database."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name
    
    db = AtomicDB(path)
    yield db
    
    try:
        os.unlink(path)
        schema_dir = os.path.join(os.path.dirname(path), "schemas")
        metadata_dir = os.path.join(os.path.dirname(path), "metadata")
        if os.path.exists(schema_dir):
            for f in os.listdir(schema_dir):
                os.unlink(os.path.join(schema_dir, f))
            os.rmdir(schema_dir)
        if os.path.exists(metadata_dir):
            for f in os.listdir(metadata_dir):
                os.unlink(os.path.join(metadata_dir, f))
            os.rmdir(metadata_dir)
    except OSError:
        pass

def test_create_collection(db):
    """Test creating a collection with schema."""
    schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
            "email": {"type": "string", "format": "email"}
        },
        "required": ["name", "email"]
    }
    
    db.create_collection("users", schema)
    
    # Valid document
    doc_id = db.insert({
        "name": "John Doe",
        "age": 30,
        "email": "john@example.com"
    }, "users")
    
    assert doc_id is not None
    
    # Invalid document (missing required field)
    with pytest.raises(ValidationError):
        db.insert({
            "name": "Jane Doe",
            "age": 25
        }, "users")
    
    # Invalid document (wrong type)
    with pytest.raises(ValidationError):
        db.insert({
            "name": "Bob",
            "age": "thirty",
            "email": "bob@example.com"
        }, "users")

def test_collection_metadata(db):
    """Test collection metadata updates."""
    schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"}
        }
    }
    
    db.create_collection("test", schema)
    
    # Insert some documents
    for i in range(5):
        db.insert({"name": f"Test {i}"}, "test")
    
    # Check metadata
    metadata = db._schema_manager.get_metadata("test")
    assert metadata is not None
    assert metadata["document_count"] == 5
    assert metadata["stats"]["last_updated"] is not None

def test_relationships(db):
    """Test adding relationships between collections."""
    # Create users collection
    user_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"}
        }
    }
    db.create_collection("users", user_schema)
    
    # Create posts collection
    post_schema = {
        "type": "object",
        "properties": {
            "title": {"type": "string"},
            "user_id": {"type": "integer"}
        }
    }
    db.create_collection("posts", post_schema)
    
    # Add relationship
    relationship = {
        "type": "one_to_many",
        "from_collection": "users",
        "to_collection": "posts",
        "foreign_key": "user_id"
    }
    db._schema_manager.add_relationship("users", relationship)
    
    # Check relationship was added
    metadata = db._schema_manager.get_metadata("users")
    assert len(metadata["relationships"]) == 1
    assert metadata["relationships"][0]["type"] == "one_to_many"
