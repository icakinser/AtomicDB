from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
import json
import os
import zlib

class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    def load(self) -> List[Dict]:
        """Load all documents from storage."""
        pass
    
    @abstractmethod
    def save(self, documents: List[Dict]):
        """Save all documents to storage."""
        pass
    
    @abstractmethod
    def close(self):
        """Close any open resources."""
        pass

class JSONStorage(StorageBackend):
    """JSON file storage backend with compression support."""
    
    def __init__(self, path: str, compression_level: int = 6):
        """Initialize JSON storage with optional compression.
        
        Args:
            path: Path to the JSON file
            compression_level: Compression level (0-9, 0=none, 9=max)
        """
        self.path = path
        self.compression_level = compression_level
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(path), exist_ok=True)
    
    def load(self) -> List[Dict]:
        """Load data from a potentially compressed JSON file."""
        if not os.path.exists(self.path):
            return []
        
        try:
            # Try to read as compressed data
            with open(self.path, 'rb') as f:
                data = f.read()
                try:
                    decompressed = zlib.decompress(data)
                    return json.loads(decompressed)
                except zlib.error:
                    # Not compressed, try reading as plain JSON
                    return json.loads(data.decode())
        except:
            # If binary read fails, try plain JSON
            with open(self.path, 'r') as f:
                return json.load(f)
    
    def save(self, documents: List[Dict]):
        """Save data to a compressed JSON file."""
        json_str = json.dumps(documents)
        if self.compression_level > 0:
            compressed = zlib.compress(json_str.encode(), level=self.compression_level)
            with open(self.path, 'wb') as f:
                f.write(compressed)
        else:
            with open(self.path, 'w') as f:
                f.write(json_str)
    
    def close(self):
        """Close storage connection (no-op for file storage)."""
        pass

class CompressedJSONStorage(JSONStorage):
    """JSON storage with default compression enabled."""
    
    def __init__(self, path: str, compression_level: int = 6):
        """Initialize compressed JSON storage.
        
        Args:
            path: Path to the JSON file
            compression_level: Compression level (1-9, default=6)
        """
        if compression_level < 1:
            raise ValueError("Compression level must be between 1 and 9")
        super().__init__(path, compression_level)

class EncryptedJSONStorage(JSONStorage):
    """JSON storage with encryption support."""
    
    def __init__(self, path: str, security_manager, compression_level: int = 6):
        """Initialize encrypted JSON storage.
        
        Args:
            path: Path to the JSON file
            security_manager: SecurityManager instance for encryption
            compression_level: Compression level (0-9, default=6)
        """
        super().__init__(path, compression_level)
        self._security = security_manager
    
    def save(self, data: Any):
        """Save encrypted data to storage."""
        # First convert to JSON
        json_str = json.dumps(data)
        
        # Compress if needed
        if self.compression_level > 0:
            json_bytes = zlib.compress(json_str.encode(), level=self.compression_level)
        else:
            json_bytes = json_str.encode()
        
        # Encrypt
        encrypted = self._security.encrypt(json_bytes)
        
        # Save to file
        with open(self.path, 'wb') as f:
            f.write(encrypted)
    
    def load(self) -> Any:
        """Load and decrypt data from storage."""
        if not os.path.exists(self.path):
            return []
        
        try:
            # Read encrypted data
            with open(self.path, 'rb') as f:
                encrypted = f.read()
            
            # Decrypt
            decrypted = self._security.decrypt(encrypted)
            
            # Decompress if needed
            try:
                if self.compression_level > 0:
                    json_str = zlib.decompress(decrypted).decode()
                else:
                    json_str = decrypted.decode()
            except zlib.error:
                # Not compressed
                json_str = decrypted.decode()
            
            # Parse JSON
            return json.loads(json_str)
        except Exception as e:
            raise ValueError(f"Failed to load encrypted data: {str(e)}")

try:
    import msgpack

    class MessagePackStorage(StorageBackend):
        """MessagePack binary storage backend."""
        
        def __init__(self, path: str):
            self.path = path
        
        def load(self) -> List[Dict]:
            if os.path.exists(self.path):
                try:
                    with open(self.path, 'rb') as f:
                        return msgpack.load(f)
                except Exception:
                    return []
            return []
        
        def save(self, documents: List[Dict]):
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(self.path, 'wb') as f:
                msgpack.dump(documents, f)
        
        def close(self):
            pass
except ImportError:
    pass

try:
    import sqlite3
    from contextlib import contextmanager
    
    class SQLiteStorage(StorageBackend):
        """SQLite storage backend."""
        
        def __init__(self, path: str):
            self.path = path
            self.conn = sqlite3.connect(path)
            self._create_table()
        
        def _create_table(self):
            with self._get_cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS documents (
                        id INTEGER PRIMARY KEY,
                        data TEXT NOT NULL
                    )
                """)
        
        @contextmanager
        def _get_cursor(self):
            cur = self.conn.cursor()
            try:
                yield cur
                self.conn.commit()
            finally:
                cur.close()
        
        def load(self) -> List[Dict]:
            with self._get_cursor() as cur:
                cur.execute("SELECT data FROM documents ORDER BY id")
                return [json.loads(row[0]) for row in cur.fetchall()]
        
        def save(self, documents: List[Dict]):
            with self._get_cursor() as cur:
                cur.execute("DELETE FROM documents")
                cur.executemany(
                    "INSERT INTO documents (data) VALUES (?)",
                    [(json.dumps(doc),) for doc in documents]
                )
        
        def close(self):
            self.conn.close()
except ImportError:
    pass

try:
    import lmdb
    
    class LMDBStorage(StorageBackend):
        """LMDB storage backend."""
        
        def __init__(self, path: str):
            os.makedirs(path, exist_ok=True)
            self.env = lmdb.open(path, map_size=10*1024*1024)  # 10MB
        
        def load(self) -> List[Dict]:
            documents = []
            with self.env.begin() as txn:
                cursor = txn.cursor()
                for key, value in cursor:
                    documents.append(json.loads(value.decode()))
            return documents
        
        def save(self, documents: List[Dict]):
            with self.env.begin(write=True) as txn:
                # Clear existing data
                cursor = txn.cursor()
                if cursor.first():
                    cursor.delete(cursor.key())
                while cursor.next():
                    cursor.delete(cursor.key())
                
                # Write new data
                for i, doc in enumerate(documents):
                    key = str(i).encode()
                    value = json.dumps(doc).encode()
                    txn.put(key, value)
        
        def close(self):
            self.env.close()
except ImportError:
    pass
