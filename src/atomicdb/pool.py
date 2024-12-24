from typing import Dict, Optional, List, Any
import threading
import queue
import time
from .database import AtomicDB
from .storage import StorageBackend

class DatabaseConnection:
    """Represents a single database connection with thread safety."""
    
    def __init__(self, path: Optional[str] = None, storage: Optional[StorageBackend] = None):
        """Initialize database connection.
        
        Args:
            path: Optional path to database file
            storage: Optional storage backend
        """
        self.db = AtomicDB(path, storage)
        self._lock = threading.RLock()
        self.last_used = time.time()
        self.in_use = False
    
    def __enter__(self):
        """Context manager entry."""
        self._lock.acquire()
        return self.db
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.last_used = time.time()
        self._lock.release()

class ConnectionPool:
    """Thread-safe connection pool for AtomicDB."""
    
    def __init__(self, path: Optional[str] = None, storage: Optional[StorageBackend] = None,
                 max_connections: int = 10, timeout: float = 30.0):
        """Initialize connection pool.
        
        Args:
            path: Optional path to database file
            storage: Optional storage backend
            max_connections: Maximum number of connections in pool
            timeout: Timeout in seconds for waiting for connection
        """
        self._path = path
        self._storage = storage
        self._max_connections = max_connections
        self._timeout = timeout
        
        self._available: queue.Queue[DatabaseConnection] = queue.Queue()
        self._in_use: List[DatabaseConnection] = []
        self._lock = threading.Lock()
        
        # Create initial connection
        self._create_connection()
    
    def _create_connection(self) -> DatabaseConnection:
        """Create a new database connection.
        
        Returns:
            New DatabaseConnection instance
            
        Raises:
            RuntimeError: If maximum connections reached
        """
        with self._lock:
            if len(self._in_use) + self._available.qsize() >= self._max_connections:
                raise RuntimeError(f"Maximum connections ({self._max_connections}) reached")
            
            conn = DatabaseConnection(self._path, self._storage)
            self._available.put(conn)
            return conn
    
    def get_connection(self) -> DatabaseConnection:
        """Get a connection from the pool.
        
        Returns:
            DatabaseConnection from pool
            
        Raises:
            queue.Empty: If timeout reached while waiting for connection
        """
        try:
            # Try to get existing connection
            conn = self._available.get(timeout=self._timeout)
        except queue.Empty:
            # Create new connection if possible
            with self._lock:
                try:
                    if len(self._in_use) + self._available.qsize() < self._max_connections:
                        conn = self._create_connection()
                        conn = self._available.get_nowait()
                    else:
                        raise queue.Empty()
                except (RuntimeError, queue.Empty):
                    # Wait for existing connection with timeout
                    conn = self._available.get(timeout=self._timeout)
        
        with self._lock:
            conn.in_use = True
            self._in_use.append(conn)
        return conn
    
    def return_connection(self, conn: DatabaseConnection):
        """Return connection to pool.
        
        Args:
            conn: Connection to return
        """
        with self._lock:
            if conn in self._in_use:
                self._in_use.remove(conn)
                conn.in_use = False
                conn.last_used = time.time()
                self._available.put(conn)
    
    def close_all(self):
        """Close all connections in pool."""
        with self._lock:
            # Close in-use connections
            while self._in_use:
                conn = self._in_use.pop()
                if hasattr(conn.db, 'close'):
                    conn.db.close()
            
            # Close available connections
            while not self._available.empty():
                conn = self._available.get_nowait()
                if hasattr(conn.db, 'close'):
                    conn.db.close()

class ThreadSafeDatabase:
    """Thread-safe wrapper for AtomicDB using connection pool."""
    
    def __init__(self, path: Optional[str] = None, storage: Optional[StorageBackend] = None,
                 max_connections: int = 10, timeout: float = 30.0):
        """Initialize thread-safe database.
        
        Args:
            path: Optional path to database file
            storage: Optional storage backend
            max_connections: Maximum number of connections in pool
            timeout: Timeout in seconds for waiting for connection
        """
        self._pool = ConnectionPool(path, storage, max_connections, timeout)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def execute(self, operation):
        """Execute operation with connection from pool.
        
        Args:
            operation: Callable that takes a database instance
            
        Returns:
            Result of operation
        """
        conn = self._pool.get_connection()
        try:
            with conn as db:
                result = operation(db)
            return result
        finally:
            self._pool.return_connection(conn)
    
    def atomic_update(self, update_operation):
        """Execute update operation atomically.
        
        Args:
            update_operation: Callable that takes current document and returns update
            
        Returns:
            Result of update
        """
        def atomic_op(db):
            with db._storage._lock:  # Access internal lock for atomic operation
                doc = db.get(lambda x: True).first()
                if doc is not None:
                    updates = update_operation(doc)
                    db.update(updates, lambda x: True)
                return doc
        return self.execute(atomic_op)
    
    def insert(self, document: Dict[str, Any]) -> int:
        """Thread-safe insert operation."""
        return self.execute(lambda db: db.insert(document))
    
    def insert_many(self, documents: List[Dict[str, Any]]) -> List[int]:
        """Thread-safe insert_many operation."""
        return self.execute(lambda db: db.insert_many(documents))
    
    def get(self, query):
        """Thread-safe get operation."""
        return self.execute(lambda db: db.get(query))
    
    def search(self, query):
        """Thread-safe search operation."""
        return self.execute(lambda db: db.search(query))
    
    def update(self, fields: Dict[str, Any], query):
        """Thread-safe update operation."""
        return self.execute(lambda db: db.update(fields, query))
    
    def remove(self, query):
        """Thread-safe remove operation."""
        return self.execute(lambda db: db.remove(query))
    
    def clear(self):
        """Thread-safe clear operation."""
        return self.execute(lambda db: db.clear())
    
    def query(self):
        """Create new query."""
        return self.execute(lambda db: db.query())
    
    def close(self):
        """Close all connections."""
        self._pool.close_all()
