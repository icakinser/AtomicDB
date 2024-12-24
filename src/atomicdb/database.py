from typing import Dict, Any, Optional, List, Callable, Union
import os
from pathlib import Path
from .storage import StorageBackend, JSONStorage, MemoryStorage
from .queries import Query, Field
from .indexes import IndexManager
from .schema import Schema, ValidationError
from .results import QueryResult

class AtomicDB:
    """A lightweight, thread-safe document database with SQL-like queries."""
    
    def __init__(self, path: Optional[Union[str, Path]] = None, storage: Optional[StorageBackend] = None, in_memory: bool = False):
        """Initialize database with storage path and backend.
        
        Args:
            path: Optional path to database file. If None and in_memory is False, uses temporary file
            storage: Optional storage backend. If None, uses MemoryStorage if in_memory=True, else JSONStorage
            in_memory: If True, uses in-memory storage. Data will be lost when database is closed unless committed
        """
        self.path = Path(path) if path else None
        
        # Set up storage backend
        if storage:
            self._storage = storage
        elif in_memory:
            self._storage = MemoryStorage(self.path)
        else:
            self._storage = JSONStorage(self.path) if self.path else MemoryStorage()
        
        self._collections: Dict[str, List[Dict]] = {"default": []}
        self._indexes = IndexManager()
        self._schema = Schema({})  # Default empty schema
        self._load()
    
    def _load(self):
        """Load data from storage."""
        try:
            data = self._storage.load()
            if data:
                self._collections = data
        except (FileNotFoundError, json.JSONDecodeError):
            pass
    
    def _save(self):
        """Save data to storage."""
        self._storage.save(self._collections)
    
    def commit(self) -> None:
        """Commit current state to disk if storage supports it."""
        if hasattr(self._storage, 'commit'):
            self._storage.commit()
    
    def close(self) -> None:
        """Close database and storage."""
        if hasattr(self._storage, 'close'):
            self._storage.close()
    
    def create_collection(self, name: str, schema: Dict[str, Any]):
        """Create a new collection with schema."""
        self._schema.create_collection(name, schema)
        if name not in self._collections:
            self._collections[name] = []
    
    def insert(self, document: Dict[str, Any], collection: str = "default"):
        """Insert a document into the database."""
        # Validate document against schema
        self._schema.validate_document(collection, document)
        
        # Ensure collection exists
        if collection not in self._collections:
            self._collections[collection] = []
        
        # Insert document
        self._collections[collection].append(document)
        doc_id = len(self._collections[collection]) - 1
        
        # Update indexes
        self._indexes.add_document(doc_id, document)
        
        # Update metadata
        stats = {
            "document_count": len(self._collections[collection]),
            "avg_document_size": len(str(document)),
            "total_size": sum(len(str(d)) for d in self._collections[collection])
        }
        self._schema.update_metadata(collection, stats)
        
        # Save changes
        self._save()
        return doc_id
    
    def insert_multiple(self, documents: List[Dict], collection: str = "default") -> List[int]:
        """Insert multiple documents and return their IDs."""
        doc_ids = []
        for doc in documents:
            doc_ids.append(self.insert(doc, collection))
        return doc_ids
    
    def insert_many(self, documents: List[Dict[str, Any]], collection: str = "default") -> List[int]:
        """Insert multiple documents into the database.
        
        Args:
            documents: List of documents to insert
            collection: Collection to insert into
            
        Returns:
            List of document IDs
        """
        doc_ids = []
        for doc in documents:
            doc_ids.append(self.insert(doc, collection))
        return doc_ids
    
    def get(self, query: Union[Query, Callable[[Dict], bool]], collection: str = "default") -> QueryResult:
        """Get the first document that matches the query."""
        if isinstance(query, Query):
            # Try to use index for the query
            conditions = query.get_equality_conditions()
            if conditions:
                fields = list(conditions.keys())
                if self._indexes.has_index(fields):
                    doc_id = self._indexes.find_one(fields, [conditions[f] for f in fields])
                    if doc_id is not None and doc_id < len(self._collections[collection]):
                        return QueryResult([self._collections[collection][doc_id].copy()], self)
            
            # Fall back to full scan
            docs = [doc for doc in self._collections[collection] if query.match(doc)]
            return QueryResult(docs, self)
        else:
            # Use callable query
            docs = [doc for doc in self._collections[collection] if query(doc)]
            return QueryResult(docs, self)
    
    def search(self, query: Union[Query, Callable[[Dict], bool]], collection: str = "default") -> QueryResult:
        """Search for all documents that match the query."""
        if isinstance(query, Query):
            # Try to use index for the query
            conditions = query.get_equality_conditions()
            if conditions:
                fields = list(conditions.keys())
                if self._indexes.has_index(fields):
                    index = self._indexes.get_index(fields)
                    if index:
                        doc_ids = index.find_all([conditions[f] for f in fields])
                        docs = [self._collections[collection][doc_id].copy() for doc_id in doc_ids if doc_id < len(self._collections[collection])]
                        return QueryResult(docs, self)
            
            # Fall back to full scan
            docs = [doc for doc in self._collections[collection] if query.match(doc)]
            return QueryResult(docs, self)
        else:
            # Use callable query
            docs = [doc for doc in self._collections[collection] if query(doc)]
            return QueryResult(docs, self)
    
    def update(self, fields: Dict[str, Any], query: Union[Query, Callable[[Dict], bool]], collection: str = "default") -> int:
        """Update documents that match the query."""
        count = 0
        if isinstance(query, Query):
            # Try to use index for the query
            conditions = query.get_equality_conditions()
            if conditions:
                fields_list = list(conditions.keys())
                if self._indexes.has_index(fields_list):
                    doc_ids = self._indexes.find_all(fields_list, [conditions[f] for f in fields_list])
                    for doc_id in doc_ids:
                        if doc_id < len(self._collections[collection]):
                            doc = self._collections[collection][doc_id]
                            old_doc = doc.copy()
                            doc.update(fields)
                            self._indexes.update_document(doc_id, old_doc, doc)
                            count += 1
                    if count > 0:
                        self._save()
                    return count
            
            # Fall back to full scan
            for i, doc in enumerate(self._collections[collection]):
                if query.match(doc):
                    old_doc = doc.copy()
                    doc.update(fields)
                    self._indexes.update_document(i, old_doc, doc)
                    count += 1
        else:
            # Use callable query
            for i, doc in enumerate(self._collections[collection]):
                if query(doc):
                    old_doc = doc.copy()
                    doc.update(fields)
                    self._indexes.update_document(i, old_doc, doc)
                    count += 1
        
        if count > 0:
            self._save()
        return count
    
    def remove(self, query: Union[Query, Callable[[Dict], bool]], collection: str = "default") -> int:
        """Remove documents that match the query."""
        count = 0
        i = 0
        while i < len(self._collections[collection]):
            doc = self._collections[collection][i]
            if isinstance(query, Query):
                matches = query.match(doc)
            else:
                matches = query(doc)
            
            if matches:
                self._collections[collection].pop(i)
                self._indexes.remove_document(i, doc)
                count += 1
            else:
                i += 1
        
        if count > 0:
            self._save()
        return count
    
    def clear(self, collection: str = "default") -> None:
        """Clear all documents from collection.
        
        Args:
            collection: Collection to clear
        """
        if collection in self._collections:
            self._collections[collection] = []
            self._save()
            
        if hasattr(self._storage, "clear"):
            self._storage.clear()

    def find(self, query: Optional[Dict[str, Any]] = None, collection: str = "default") -> QueryResult:
        """Find documents matching query.
        
        Args:
            query: Query to match documents against
            collection: Collection to search in
            
        Returns:
            QueryResult containing matching documents
        """
        if collection not in self._collections:
            return QueryResult([])
            
        documents = self._collections[collection]
        if not query:
            return QueryResult(documents)
            
        matches = []
        for doc in documents:
            if self._matches_query(doc, query):
                matches.append(doc)
        return QueryResult(matches)
    
    def find_one(self, query: Dict[str, Any], collection: str = "default") -> Optional[Dict[str, Any]]:
        """Find first document matching query.
        
        Args:
            query: Query to match documents against
            collection: Collection to search in
            
        Returns:
            First matching document or None
        """
        result = self.find(query, collection)
        return result[0] if result else None
    
    def _matches_query(self, document: Dict[str, Any], query: Dict[str, Any]) -> bool:
        """Check if document matches query.
        
        Args:
            document: Document to check
            query: Query to match against
            
        Returns:
            True if document matches query
        """
        for field, value in query.items():
            if isinstance(value, dict):
                # Handle operators
                for op, op_value in value.items():
                    if not self._apply_operator(document.get(field), op, op_value):
                        return False
            elif document.get(field) != value:
                return False
        return True
    
    def _apply_operator(self, field_value: Any, op: str, op_value: Any) -> bool:
        """Apply query operator.
        
        Args:
            field_value: Value from document
            op: Operator to apply
            op_value: Value to compare against
            
        Returns:
            True if operator condition is met
        """
        if op == "$eq":
            return field_value == op_value
        elif op == "$ne":
            return field_value != op_value
        elif op == "$gt":
            return field_value > op_value
        elif op == "$gte":
            return field_value >= op_value
        elif op == "$lt":
            return field_value < op_value
        elif op == "$lte":
            return field_value <= op_value
        elif op == "$in":
            return field_value in op_value
        elif op == "$nin":
            return field_value not in op_value
        elif op == "$exists":
            return (field_value is not None) == op_value
        elif op == "$regex":
            import re
            return bool(re.search(op_value, str(field_value)))
        return False
    
    def contains(self, query: Union[Query, Callable[[Dict], bool]], collection: str = "default") -> bool:
        """Check if any document matches the query."""
        return self.get(query, collection).count() > 0
    
    def count(self, query: Optional[Union[Query, Callable[[Dict], bool]]] = None, collection: str = "default") -> int:
        """Count documents that match the query."""
        if query is None:
            return len(self._collections[collection])
        return self.search(query, collection).count()
    
    def all(self, collection: str = "default") -> QueryResult:
        """Get all documents."""
        return QueryResult([doc.copy() for doc in self._collections[collection]], self)
    
    def document_ids(self, collection: str = "default") -> List[int]:
        """Get list of all document IDs."""
        return list(range(len(self._collections[collection])))
    
    def create_index(self, *fields: str):
        """Create an index on specified fields."""
        self._indexes.create_index(list(fields))
        # Build index for existing documents
        for collection in self._collections.values():
            for i, doc in enumerate(collection):
                self._indexes.add_document(i, doc)
    
    def drop_index(self, *fields: str):
        """Drop the index on specified fields."""
        self._indexes.drop_index(list(fields))
    
    def query(self) -> Query:
        """Create a new query builder."""
        return Query()
