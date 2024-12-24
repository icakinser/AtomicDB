"""Base storage backend interface."""

from abc import ABC, abstractmethod
from typing import Dict, List, Any

class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    def load(self) -> List[Dict[str, Any]]:
        """Load data from storage."""
        pass
    
    @abstractmethod
    def save(self, data: List[Dict[str, Any]]) -> None:
        """Save data to storage."""
        pass
    
    @abstractmethod
    def clear(self) -> None:
        """Clear all data from storage."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close storage and free resources."""
        pass
