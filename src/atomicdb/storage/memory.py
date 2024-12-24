from typing import Dict, List, Any, Optional
import json
from .base import StorageBackend

class MemoryStorage(StorageBackend):
    """In-memory storage backend that optionally syncs to disk."""
    
    def __init__(self, path: Optional[str] = None):
        """Initialize memory storage.
        
        Args:
            path: Optional path to persist data to disk
        """
        self.path = path
        self._data: List[Dict[str, Any]] = []
    
    def load(self) -> List[Dict[str, Any]]:
        """Load data from memory, or from disk if path exists."""
        if self.path and self.path.exists():
            with open(self.path, 'r') as f:
                self._data = json.load(f)
        return self._data
    
    def save(self, data: List[Dict[str, Any]]) -> None:
        """Save data to memory."""
        self._data = data
    
    def commit(self) -> None:
        """Commit in-memory data to disk if path is set."""
        if self.path:
            with open(self.path, 'w') as f:
                json.dump(self._data, f, indent=2)
    
    def clear(self) -> None:
        """Clear all data from memory."""
        self._data = []
        if self.path and self.path.exists():
            self.path.unlink()
    
    def close(self) -> None:
        """Close storage and optionally commit to disk."""
        pass  # Memory storage doesn't need explicit closing
