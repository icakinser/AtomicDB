"""Query result wrapper for AtomicDB."""

from typing import Dict, Any, List, Optional, Iterator, Union

class QueryResult:
    """Wrapper for database query results."""
    
    def __init__(self, documents: List[Dict[str, Any]]):
        """Initialize query result.
        
        Args:
            documents: List of documents from query
        """
        self._documents = documents
    
    def __len__(self) -> int:
        """Get number of documents."""
        return len(self._documents)
    
    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Iterate over documents."""
        return iter(self._documents)
    
    def __getitem__(self, index: Union[int, slice]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Get document by index."""
        return self._documents[index]
    
    def first(self) -> Optional[Dict[str, Any]]:
        """Get first document."""
        return self._documents[0] if self._documents else None
    
    def last(self) -> Optional[Dict[str, Any]]:
        """Get last document."""
        return self._documents[-1] if self._documents else None
    
    def pluck(self, *fields: str) -> List[Dict[str, Any]]:
        """Get only specified fields from documents.
        
        Args:
            fields: Fields to include in result
            
        Returns:
            List of documents with only specified fields
        """
        result = []
        for doc in self._documents:
            plucked = {}
            for field in fields:
                if field in doc:
                    plucked[field] = doc[field]
            result.append(plucked)
        return result
    
    def exclude(self, *fields: str) -> List[Dict[str, Any]]:
        """Get documents without specified fields.
        
        Args:
            fields: Fields to exclude from result
            
        Returns:
            List of documents without specified fields
        """
        result = []
        for doc in self._documents:
            excluded = doc.copy()
            for field in fields:
                excluded.pop(field, None)
            result.append(excluded)
        return result
    
    def sort_by(self, field: str, reverse: bool = False) -> 'QueryResult':
        """Sort documents by field.
        
        Args:
            field: Field to sort by
            reverse: If True, sort in descending order
            
        Returns:
            New QueryResult with sorted documents
        """
        sorted_docs = sorted(self._documents, key=lambda x: x.get(field), reverse=reverse)
        return QueryResult(sorted_docs)
    
    def as_list(self) -> List[Dict[str, Any]]:
        """Get raw list of documents."""
        return self._documents
    
    def count(self) -> int:
        """Get number of documents."""
        return len(self._documents)
    
    def is_empty(self) -> bool:
        """Check if result is empty."""
        return len(self._documents) == 0
