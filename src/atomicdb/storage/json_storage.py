"""JSON file storage backend."""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from .base import StorageBackend

class JSONStorage(StorageBackend):
    """JSON file storage backend."""
    
    def __init__(self, path: Optional[Union[str, Path]] = None):
        """Initialize JSON storage.
        
        Args:
            path: Path to JSON file
        """
        self.path = Path(path) if path else None
    
    def load(self) -> List[Dict[str, Any]]:
        """Load data from JSON file."""
        if not self.path or not self.path.exists():
            return []
        
        with open(self.path, 'r') as f:
            return json.load(f)
    
    def save(self, data: List[Dict[str, Any]]) -> None:
        """Save data to JSON file."""
        if not self.path:
            return
            
        with open(self.path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def clear(self) -> None:
        """Clear JSON file."""
        if self.path and self.path.exists():
            self.path.unlink()
    
    def close(self) -> None:
        """Close storage."""
        pass  # No need to explicitly close JSON files
