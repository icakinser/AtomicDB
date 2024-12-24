import base64
import hashlib
import os
from typing import Union, Optional
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

class SecurityManager:
    """Manages encryption and password hashing for FLatDB."""
    
    def __init__(self, password: Optional[str] = None, salt: Optional[bytes] = None):
        """Initialize security manager with optional password and salt.
        
        Args:
            password: Optional password for encryption
            salt: Optional salt for key derivation. If not provided, a new one is generated.
        """
        self._salt = salt if salt is not None else os.urandom(16)
        self._key = None
        if password:
            self.set_password(password)
    
    def set_password(self, password: str) -> None:
        """Set encryption password and derive key.
        
        Args:
            password: Password to use for encryption
        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=self._salt,
            iterations=480000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        self._key = key
        self._fernet = Fernet(key)
    
    @property
    def salt(self) -> bytes:
        """Get the salt used for key derivation."""
        return self._salt
    
    def hash_password(self, password: str) -> str:
        """Hash a password using SHA-256 and salt.
        
        Args:
            password: Password to hash
        
        Returns:
            Hashed password as hex string
        """
        return hashlib.sha256(password.encode() + self._salt).hexdigest()
    
    def verify_password(self, password: str, hashed: str) -> bool:
        """Verify a password against its hash.
        
        Args:
            password: Password to verify
            hashed: Previously hashed password
        
        Returns:
            True if password matches hash
        """
        return self.hash_password(password) == hashed
    
    def encrypt(self, data: Union[str, bytes]) -> bytes:
        """Encrypt data using Fernet symmetric encryption.
        
        Args:
            data: Data to encrypt (string or bytes)
        
        Returns:
            Encrypted data as bytes
        
        Raises:
            ValueError: If no encryption key is set
        """
        if self._key is None:
            raise ValueError("No encryption key set. Call set_password first.")
        
        if isinstance(data, str):
            data = data.encode()
        return self._fernet.encrypt(data)
    
    def decrypt(self, data: bytes) -> bytes:
        """Decrypt data using Fernet symmetric encryption.
        
        Args:
            data: Encrypted data to decrypt
        
        Returns:
            Decrypted data as bytes
        
        Raises:
            ValueError: If no encryption key is set
        """
        if self._key is None:
            raise ValueError("No encryption key set. Call set_password first.")
        
        return self._fernet.decrypt(data)
