"""Schema validation for AtomicDB."""

from typing import Dict, Any, Optional

class ValidationError(Exception):
    """Raised when document validation fails."""
    pass

class Schema:
    """Schema validator for AtomicDB collections."""
    
    def __init__(self, schema: Dict[str, Any]):
        """Initialize schema validator.
        
        Args:
            schema: Schema definition
        """
        self._schemas: Dict[str, Dict[str, Any]] = {
            "default": {}  # Default collection has no schema
        }
        self._metadata: Dict[str, Dict[str, Any]] = {
            "default": {}  # Default collection metadata
        }
    
    def create_collection(self, name: str, schema: Dict[str, Any]) -> None:
        """Create a new collection with schema.
        
        Args:
            name: Collection name
            schema: Schema definition
        """
        self._schemas[name] = schema
        self._metadata[name] = {}
    
    def validate_document(self, collection: str, document: Dict[str, Any]) -> None:
        """Validate document against collection schema.
        
        Args:
            collection: Collection name
            document: Document to validate
            
        Raises:
            ValidationError: If validation fails
        """
        if collection not in self._schemas:
            return  # No schema validation for undefined collections
            
        schema = self._schemas[collection]
        if not schema:
            return  # Empty schema means no validation
            
        self._validate_against_schema(document, schema)
    
    def _validate_against_schema(self, document: Dict[str, Any], schema: Dict[str, Any]) -> None:
        """Validate document against schema definition.
        
        Args:
            document: Document to validate
            schema: Schema definition
            
        Raises:
            ValidationError: If validation fails
        """
        for field, field_schema in schema.items():
            if field not in document:
                if field_schema.get("required", False):
                    raise ValidationError(f"Missing required field: {field}")
                continue
                
            value = document[field]
            field_type = field_schema.get("type")
            
            if field_type == "string" and not isinstance(value, str):
                raise ValidationError(f"Field {field} must be a string")
            elif field_type == "number" and not isinstance(value, (int, float)):
                raise ValidationError(f"Field {field} must be a number")
            elif field_type == "boolean" and not isinstance(value, bool):
                raise ValidationError(f"Field {field} must be a boolean")
            elif field_type == "object" and not isinstance(value, dict):
                raise ValidationError(f"Field {field} must be an object")
            elif field_type == "array" and not isinstance(value, list):
                raise ValidationError(f"Field {field} must be an array")
    
    def update_metadata(self, collection: str, metadata: Dict[str, Any]) -> None:
        """Update collection metadata.
        
        Args:
            collection: Collection name
            metadata: Metadata to update
        """
        if collection not in self._metadata:
            self._metadata[collection] = {}
        self._metadata[collection].update(metadata)
