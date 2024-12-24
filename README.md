# AtomicDB

A lightweight, thread-safe document database with SQL-like queries, implemented in pure Python.

## Features

- **Document Storage**: Store and query JSON documents with a simple API
- **Thread Safety**: Built-in connection pooling and atomic operations
- **SQL-like Queries**: Familiar query syntax with operators like `==`, `>`, `<`, `contains`
- **Schema Validation**: Optional schema validation for documents
- **Security**: Built-in encryption support
- **List-like Results**: Query results behave like Python lists with additional helper methods
- **Zero Dependencies**: Core functionality requires only Python standard library
- **Optional Features**: Encryption support via `cryptography` package

## Installation

```bash
pip install atomicdb
```

## Quick Start

```python
from atomicdb import AtomicDB

# Create or open a database
db = AtomicDB("users.json")

# Insert documents
db.insert({"name": "Alice", "age": 30, "email": "alice@example.com"})
db.insert({"name": "Bob", "age": 25, "email": "bob@example.com"})

# Query documents
young_users = db.search(db.query().age < 30)
print(young_users.first()["name"])  # "Bob"

# Update documents
db.update({"age": 31}, db.query().name == "Alice")

# Delete documents
db.remove(db.query().age > 30)
```

## Thread Safety

AtomicDB provides thread-safe operations through connection pooling:

```python
from atomicdb import ThreadSafeDatabase

# Create thread-safe database with connection pool
db = ThreadSafeDatabase("users.json", max_connections=5)

# All operations are automatically thread-safe
db.insert({"name": "Charlie"})
results = db.search(db.query().name == "Charlie")

# Atomic updates prevent race conditions
def update_counter(doc):
    return {"counter": doc["counter"] + 1}
db.atomic_update(update_counter)
```

## Querying with Query Builder

```python
# Create a query
query = db.query()

# Simple equality
results = db.search(query.name == 'John')

# Comparison operators
adults = db.search(query.age >= 18)
young_adults = db.search((query.age >= 18) & (query.age <= 25))

# Pattern matching with regex
j_names = db.search(query.name.matches(r'J.*'))

# List operations
age_group = db.search(query.age.in_([25, 30, 35]))
not_status = db.search(query.status.not_in(['deleted', 'archived']))

# Field existence
has_email = db.search(query.email.exists())

# Type checking
valid_ages = db.search(query.age.type_('int'))
```

### Legacy Lambda Queries
```python
# You can still use lambda functions if preferred
results = db.search(lambda x: x['age'] > 25)
```

### Updating Documents

```python
# Update with Query Builder
db.update({'age': 31}, query.name == 'John')

# Update with lambda
db.update({'status': 'active'}, lambda x: x['age'] >= 18)
```

### Removing Documents

```python
# Remove with Query Builder
db.remove(query.name == 'John')

# Remove with lambda
db.remove(lambda x: x['status'] == 'deleted')

# Remove all documents
db.clear()
```

### Other Operations

```python
# Count documents
total = db.count()
adults = db.count(query.age >= 18)

# Check existence
has_admin = db.contains(query.role == 'admin')

# Get all documents
all_docs = db.all()

# Get document IDs
ids = db.document_ids
```

### Indexing

```python
from atomicdb import AtomicDB

# Create a database
db = AtomicDB('db.json')

# Create a single-field index
db.create_index("name")

# Create a composite index on multiple fields
db.create_index("name", "age")

# Indexes are automatically maintained
db.insert({"name": "John", "age": 30})  # Index is updated
db.update({"age": 31}, db.query().name == "John")  # Index is updated
db.remove(db.query().name == "John")  # Index is updated

# Drop an index when no longer needed
db.drop_index("name")
db.drop_index("name", "age")

# Queries automatically use the best available index
results = db.search(db.query().name == "John")  # Uses name index
results = db.search((db.query().name == "John") & (db.query().age == 30))  # Uses composite index
```

### Query Performance

When you create an index on a field or set of fields, queries that use those fields will be much faster. Here are some tips for optimal performance:

1. Create indexes on fields you frequently query with equality conditions
2. Create composite indexes for queries that filter on multiple fields
3. Indexes are automatically maintained when you insert, update, or remove documents
4. Indexes use additional memory, so only create them for frequently queried fields
5. Drop indexes that are no longer needed to free up memory

## Query Operators

| Operator | Method | Example |
|----------|---------|---------|
| == | Equal | `query.age == 25` |
| != | Not Equal | `query.age != 30` |
| > | Greater Than | `query.age > 18` |
| < | Less Than | `query.age < 65` |
| >= | Greater Equal | `query.age >= 21` |
| <= | Less Equal | `query.age <= 30` |
| matches | Regex Match | `query.name.matches(r'J.*')` |
| in_ | In List | `query.age.in_([25, 30, 35])` |
| not_in | Not In List | `query.status.not_in(['deleted'])` |
| exists | Field Exists | `query.email.exists()` |
| type_ | Type Check | `query.age.type_('int')` |

## Query Results

Query results in AtomicDB are returned as `QueryResult` objects, which provide a list-like interface with additional helper methods:

```python
from atomicdb import AtomicDB

db = AtomicDB("data.json")

# Search for documents
results = db.search(db.query().age > 25)

# List-like operations
first_doc = results[0]  # Get first document
last_doc = results[-1]  # Get last document
subset = results[1:3]   # Slice results

# Helper methods
first = results.first()  # Get first document or None if empty
last = results.last()   # Get last document or None if empty
count = results.count() # Get number of documents
is_empty = results.is_empty()  # Check if results are empty

# Field operations
names = results.pluck("name")  # Get only name field
no_age = results.exclude("age")  # Exclude age field

# Sorting
by_age = results.sort_by("age")  # Sort by age ascending
by_age_desc = results.sort_by("age", reverse=True)  # Sort by age descending

# Bulk updates
results.update_all({"status": "active"})  # Update all documents

# Convert to plain list
docs_list = results.as_list()
```

The `QueryResult` class provides:
- List-like interface (indexing, slicing, iteration)
- Helper methods for common operations
- Field selection and exclusion
- Sorting capabilities
- Bulk updates
- Empty result handling

All operations maintain the database connection, allowing for automatic updates when modifying documents:

```python
# Changes are automatically saved to the database
results[0]["name"] = "New Name"  # Updates first document
results.update_all({"status": "inactive"})  # Updates all documents
```

## Storage Backends

AtomicDB supports multiple storage backends, each with its own advantages:

```python
from atomicdb import AtomicDB
from atomicdb.storage import JSONStorage, MessagePackStorage, SQLiteStorage, LMDBStorage

# Default JSON storage (human-readable, good for small datasets)
db = AtomicDB('db.json')

# MessagePack storage (binary format, faster and smaller)
db = AtomicDB('db.msgpack', storage_backend=MessagePackStorage('db.msgpack'))

# SQLite storage (ACID compliant, good for concurrent access)
db = AtomicDB('db.sqlite', storage_backend=SQLiteStorage('db.sqlite'))

# LMDB storage (memory-mapped, very fast)
db = AtomicDB('db.lmdb', storage_backend=LMDBStorage('db.lmdb'))

# Don't forget to close the database when done
db.close()
```

#### Storage Backend Comparison

| Backend    | Format  | Speed | Size | Concurrency | ACID | Use Case |
|------------|---------|-------|------|-------------|------|----------|
| JSON       | Text    | Slow  | Large| No          | No   | Development, small datasets |
| MessagePack| Binary  | Fast  | Small| No          | No   | Production, medium datasets |
| SQLite     | Binary  | Medium| Small| Yes         | Yes  | Concurrent access, data integrity |
| LMDB      | Binary  | Fast  | Small| Yes         | Yes  | High performance, large datasets |

Choose the storage backend that best fits your needs:

1. **JSON Storage**
   - Human-readable format
   - Good for development and debugging
   - Best for small datasets
   - No additional dependencies

2. **MessagePack Storage**
   - Binary format (smaller file size)
   - Faster than JSON
   - Good for medium-sized datasets
   - Requires `msgpack` package

3. **SQLite Storage**
   - ACID compliant
   - Supports concurrent access
   - Good for data integrity
   - Built into Python

4. **LMDB Storage**
   - Memory-mapped files (very fast)
   - Supports concurrent readers
   - Good for large datasets
   - Requires `lmdb` package

## In-Memory Storage

AtomicDB now supports in-memory storage for high-performance operations. You can choose between pure in-memory mode or in-memory with disk persistence.

### Pure In-Memory Mode

```python
from atomicdb import AtomicDB

# Create a pure in-memory database
db = AtomicDB(in_memory=True)

# Perform operations
db.insert({"name": "John", "age": 30})
results = db.find({"name": "John"})

# Create an in-memory database with disk backup
db = AtomicDB(path="data.json", in_memory=True)

# Perform operations in memory
db.insert({"name": "John", "age": 30})

# Commit changes to disk when needed
db.commit()

# Clear all data from memory
db.clear()

## Security and Encryption

AtomicDB supports password-based encryption and secure password hashing:

### Database Encryption

Use the `SecurityManager` and `EncryptedJSONStorage` to encrypt your database:

```python
from atomicdb import AtomicDB
from atomicdb.security import SecurityManager
from atomicdb.storage import EncryptedJSONStorage

# Create a security manager with a password
security = SecurityManager(password="your_secure_password")

# Create encrypted storage
storage = EncryptedJSONStorage("data.json", security)

# Create database with encrypted storage
db = AtomicDB(storage=storage)
```

The encryption features include:
- Symmetric encryption using Fernet (based on AES-128 in CBC mode)
- Key derivation using PBKDF2-HMAC-SHA256 with 480,000 iterations
- Secure random salt generation
- Compatible with compression (encrypts after compression)

### Password Hashing

You can also use the `SecurityManager` for secure password hashing:

```python
from atomicdb.security import SecurityManager

# Create a security manager
security = SecurityManager()

# Hash a password
password = "user_password"
hashed = security.hash_password(password)

# Verify a password
is_valid = security.verify_password(password, hashed)
```

The password hashing features include:
- SHA-256 hashing with random salt
- Protection against timing attacks
- Secure salt persistence

### Security Best Practices

1. Store the salt securely and persistently:
```python
# Save the salt when creating the security manager
security = SecurityManager(password="password")
salt = security.salt  # Save this securely

# Later, recreate the security manager with the same salt
security = SecurityManager(password="password", salt=salt)
```

2. Use strong passwords (recommend minimum 12 characters)
3. Keep the encryption key secure and never store it with the encrypted data
4. Regularly backup your encrypted database
5. Consider using environment variables for passwords in production

## Storage and Compression

AtomicDB supports multiple storage backends and compression options:

### JSON Storage

The default storage backend uses JSON format with optional compression:

```python
from atomicdb import AtomicDB
from atomicdb.storage import JSONStorage, CompressedJSONStorage

# Without compression
db = AtomicDB("data.json")

# With optional compression (level 0-9)
storage = JSONStorage("data.json", compression_level=6)
db = AtomicDB(storage=storage)

# With enforced compression
storage = CompressedJSONStorage("data.json")  # Always uses compression
db = AtomicDB(storage=storage)
```

The compression feature uses Python's built-in `zlib` library and typically achieves:
- 50% or better compression ratio for text-heavy documents
- Transparent compression/decompression
- Backward compatibility with uncompressed files

Choose a compression level based on your needs:
- Level 0: No compression (fastest)
- Level 1-3: Fast compression
- Level 4-6: Balanced compression (recommended)
- Level 7-9: Best compression (slowest)

## Combining Encryption and Compression

You can use both encryption and compression together for secure, space-efficient storage:

```python
from atomicdb import AtomicDB
from atomicdb.security import SecurityManager
from atomicdb.storage import EncryptedJSONStorage

# Create security manager
security = SecurityManager(password="your_secure_password")

# Create encrypted storage with compression
storage = EncryptedJSONStorage(
    path="data.json",
    security_manager=security,
    compression_level=6  # Compression level 0-9
)

# Create database with encrypted and compressed storage
db = AtomicDB(storage=storage)

# Data will be automatically compressed then encrypted when saved
db.insert({"large_text": "..." * 1000})

# And automatically decrypted and decompressed when loaded
data = db.get(db.query().large_text.exists())
```

The process flow is:
1. Data → JSON → Compression → Encryption → Storage
2. Storage → Decryption → Decompression → JSON → Data

Benefits of combining encryption and compression:
- Better compression ratios (compressing before encryption)
- Reduced storage space for encrypted data
- No compromise in security
- Transparent to the application code

Performance considerations:
- Compression level 0: Fastest, no compression
- Compression level 1-3: Fast compression
- Compression level 4-6: Balanced (recommended)
- Compression level 7-9: Best compression, but slower

Example with custom compression and encryption settings:
```python
# Maximum security and compression
storage = EncryptedJSONStorage(
    path="data.json",
    security_manager=SecurityManager(
        password="your_secure_password",
        # Optional: provide your own salt
        salt=b"your_secure_salt"
    ),
    compression_level=9  # Maximum compression
)

# Speed-optimized settings
storage = EncryptedJSONStorage(
    path="data.json",
    security_manager=SecurityManager(
        password="your_secure_password"
    ),
    compression_level=1  # Fast compression
)
```

## Schema and Metadata

AtomicDB supports JSON Schema validation and metadata tracking:

```python
from atomicdb import AtomicDB

# Create a database
db = AtomicDB('db.json')

# Define a schema for users
user_schema = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "integer"},
        "email": {"type": "string", "format": "email"}
    },
    "required": ["name", "email"]
}

# Create a collection with schema
db.create_collection("users", user_schema)

# Insert a valid document
db.insert({
    "name": "John Doe",
    "age": 30,
    "email": "john@example.com"
}, "users")

# This will raise an error (missing required email)
try:
    db.insert({
        "name": "Jane Doe",
        "age": 25
    }, "users")
except ValueError as e:
    print(f"Validation error: {e}")

# Define a schema for posts
post_schema = {
    "type": "object",
    "properties": {
        "title": {"type": "string"},
        "user_id": {"type": "integer"}
    }
}
db.create_collection("posts", post_schema)

# Add a relationship
relationship = {
    "type": "one_to_many",
    "from_collection": "users",
    "to_collection": "posts",
    "foreign_key": "user_id"
}
db._schema_manager.add_relationship("users", relationship)

# Get collection metadata
metadata = db._schema_manager.get_metadata("users")
print(f"Document count: {metadata['document_count']}")
print(f"Last updated: {metadata['stats']['last_updated']}")
print(f"Relationships: {metadata['relationships']}")
```

### Database Structure

AtomicDB organizes data and metadata in the following structure:

```
/db
  /schemas/
    collection_schemas.json    # Document schemas
  /metadata/
    users_metadata.json       # Collection metadata
    posts_metadata.json
  /collections/
    users.json               # Actual data
    posts.json
```

Each collection's metadata includes:
- Document count
- Average document size
- Total collection size
- Index information
- Relationship definitions
- Last update timestamp

## Development

1. Clone the repository
2. Install development dependencies: `pip install -r requirements.txt`
3. Run tests: `pytest tests/`

## Contributing

Contributions are welcome! Here are some ways you can contribute:

- Report bugs
- Suggest new features
- Submit pull requests
- Improve documentation

## License

MIT License
