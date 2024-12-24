from typing import Any, Callable, Dict, List, Optional, Union
import re

class Field:
    """Represents a field in a document for querying."""
    
    def __init__(self, name: str, parent: Optional['Query'] = None):
        """Initialize field with name and optional parent query."""
        self.name = name
        self.parent = parent
    
    def __eq__(self, value: Any) -> 'Query':
        """Create equality condition."""
        return Query(lambda doc: doc.get(self.name) == value, parent=self.parent)
    
    def __ne__(self, value: Any) -> 'Query':
        """Create inequality condition."""
        return Query(lambda doc: doc.get(self.name) != value, parent=self.parent)
    
    def __gt__(self, value: Any) -> 'Query':
        """Create greater than condition."""
        return Query(lambda doc: doc.get(self.name) > value, parent=self.parent)
    
    def __lt__(self, value: Any) -> 'Query':
        """Create less than condition."""
        return Query(lambda doc: doc.get(self.name) < value, parent=self.parent)
    
    def __ge__(self, value: Any) -> 'Query':
        """Create greater than or equal condition."""
        return Query(lambda doc: doc.get(self.name) >= value, parent=self.parent)
    
    def __le__(self, value: Any) -> 'Query':
        """Create less than or equal condition."""
        return Query(lambda doc: doc.get(self.name) <= value, parent=self.parent)
    
    def matches(self, pattern: str) -> 'Query':
        """Create regex match condition."""
        regex = re.compile(pattern)
        return Query(lambda doc: bool(regex.match(str(doc.get(self.name)))), parent=self.parent)
    
    def contains(self, value: Any) -> 'Query':
        """Create contains condition for lists."""
        return Query(lambda doc: value in doc.get(self.name, []), parent=self.parent)
    
    def exists(self) -> 'Query':
        """Create exists condition."""
        return Query(lambda doc: self.name in doc, parent=self.parent)
    
    def type(self, type_name: str) -> 'Query':
        """Create type condition."""
        type_map = {
            'str': str,
            'int': int,
            'float': float,
            'bool': bool,
            'list': list,
            'dict': dict,
            'null': type(None)
        }
        type_class = type_map.get(type_name.lower())
        if type_class is None:
            raise ValueError(f"Unknown type: {type_name}")
        return Query(lambda doc: isinstance(doc.get(self.name), type_class), parent=self.parent)

class Query:
    """Query builder for FLatDB."""
    
    def __init__(self, test: Optional[Callable[[Dict], bool]] = None, parent: Optional['Query'] = None):
        """Initialize query with test function and optional parent query."""
        self.test = test if test is not None else lambda doc: True
        self.parent = parent
    
    def __getattr__(self, name: str) -> Field:
        """Get field by name."""
        return Field(name, self)
    
    def __and__(self, other: 'Query') -> 'Query':
        """Combine queries with AND."""
        return Query(lambda doc: self.test(doc) and other.test(doc))
    
    def __or__(self, other: 'Query') -> 'Query':
        """Combine queries with OR."""
        return Query(lambda doc: self.test(doc) or other.test(doc))
    
    def __invert__(self) -> 'Query':
        """Negate query."""
        return Query(lambda doc: not self.test(doc))
    
    def match(self, document: Dict) -> bool:
        """Test if document matches query."""
        return self.test(document)
    
    def get_equality_conditions(self) -> Dict[str, Any]:
        """Get dictionary of equality conditions for indexing."""
        # This is a placeholder for more complex implementations
        return {}
