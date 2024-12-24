"""
AtomicDB: A lightweight, thread-safe document database with SQL-like queries.
"""

__version__ = "0.1.0"

from .database import AtomicDB
from .storage import StorageBackend, JSONStorage, MemoryStorage
from .schema import Schema, ValidationError
from .pool import ThreadSafeDatabase, ConnectionPool

__all__ = [
    'AtomicDB',
    'StorageBackend',
    'JSONStorage',
    'MemoryStorage',
    'Schema',
    'ValidationError',
    'ThreadSafeDatabase',
    'ConnectionPool'
]
