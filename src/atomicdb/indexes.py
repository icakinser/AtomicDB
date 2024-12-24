from typing import Dict, List, Any, Optional, Set

class Index:
    """Represents an index on one or more fields."""
    
    def __init__(self, fields: List[str]):
        self.fields = fields
        self._index: Dict[tuple, Set[int]] = {}
    
    def add_document(self, doc_id: int, doc: Dict[str, Any]):
        """Add a document to the index."""
        key = self._get_key(doc)
        if key is not None:
            if key not in self._index:
                self._index[key] = set()
            self._index[key].add(doc_id)
    
    def remove_document(self, doc_id: int, doc: Dict[str, Any]):
        """Remove a document from the index."""
        key = self._get_key(doc)
        if key is not None and key in self._index:
            self._index[key].discard(doc_id)
            if not self._index[key]:
                del self._index[key]
    
    def update_document(self, doc_id: int, old_doc: Dict[str, Any], new_doc: Dict[str, Any]):
        """Update a document in the index."""
        old_key = self._get_key(old_doc)
        new_key = self._get_key(new_doc)
        
        if old_key == new_key:
            return
        
        if old_key is not None and old_key in self._index:
            self._index[old_key].discard(doc_id)
            if not self._index[old_key]:
                del self._index[old_key]
        
        if new_key is not None:
            if new_key not in self._index:
                self._index[new_key] = set()
            self._index[new_key].add(doc_id)
    
    def find_one(self, values: List[Any]) -> Optional[int]:
        """Find one document ID matching the values."""
        key = tuple(values)
        if key in self._index:
            return next(iter(self._index[key]))
        return None
    
    def find_all(self, values: List[Any]) -> Set[int]:
        """Find all document IDs matching the values."""
        key = tuple(values)
        return self._index.get(key, set())
    
    def _get_key(self, doc: Dict[str, Any]) -> Optional[tuple]:
        """Get index key for a document."""
        try:
            return tuple(doc[field] for field in self.fields)
        except KeyError:
            return None

class IndexManager:
    """Manages indexes for the database."""
    
    def __init__(self):
        """Initialize index manager."""
        self._indexes: Dict[tuple, Index] = {}
    
    def create_index(self, fields: List[str]):
        """Create an index on specified fields."""
        key = tuple(sorted(fields))
        if key not in self._indexes:
            self._indexes[key] = Index(fields)
    
    def drop_index(self, fields: List[str]):
        """Drop the index on specified fields."""
        key = tuple(sorted(fields))
        if key in self._indexes:
            del self._indexes[key]
    
    def has_index(self, fields: List[str]) -> bool:
        """Check if an index exists for the specified fields."""
        key = tuple(sorted(fields))
        return key in self._indexes
    
    def get_index(self, fields: List[str]) -> Optional[Index]:
        """Get the index for the specified fields."""
        key = tuple(sorted(fields))
        return self._indexes.get(key)
    
    def add_document(self, doc_id: int, doc: Dict[str, Any]):
        """Add a document to all indexes."""
        for index in self._indexes.values():
            index.add_document(doc_id, doc)
    
    def remove_document(self, doc_id: int, doc: Dict[str, Any]):
        """Remove a document from all indexes."""
        for index in self._indexes.values():
            index.remove_document(doc_id, doc)
    
    def update_document(self, doc_id: int, old_doc: Dict[str, Any], new_doc: Dict[str, Any]):
        """Update a document in all indexes."""
        for index in self._indexes.values():
            index.update_document(doc_id, old_doc, new_doc)
    
    def clear(self):
        """Clear all indexes."""
        self._indexes.clear()
