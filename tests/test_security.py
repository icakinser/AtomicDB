import pytest
import os
import tempfile
from atomicdb import AtomicDB
from atomicdb.security import SecurityManager
from atomicdb.storage import EncryptedJSONStorage

@pytest.fixture
def security_manager():
    """Create a security manager for testing."""
    return SecurityManager(password="test_password")

@pytest.fixture
def temp_file(tmp_path):
    """Create a temporary file for testing."""
    return str(tmp_path / "test.json")

def test_password_hashing(security_manager):
    """Test password hashing and verification."""
    password = "my_secure_password"
    hashed = security_manager.hash_password(password)
    
    # Verify correct password
    assert security_manager.verify_password(password, hashed)
    
    # Verify incorrect password
    assert not security_manager.verify_password("wrong_password", hashed)

def test_encryption(security_manager):
    """Test data encryption and decryption."""
    data = "sensitive data"
    
    # Test string encryption
    encrypted = security_manager.encrypt(data)
    decrypted = security_manager.decrypt(encrypted)
    assert decrypted.decode() == data
    
    # Test bytes encryption
    data_bytes = b"sensitive bytes"
    encrypted = security_manager.encrypt(data_bytes)
    decrypted = security_manager.decrypt(encrypted)
    assert decrypted == data_bytes

def test_encrypted_storage(temp_file, security_manager):
    """Test encrypted JSON storage."""
    storage = EncryptedJSONStorage(temp_file, security_manager)
    data = [{"secret": "value"}]
    
    # Save encrypted data
    storage.save(data)
    assert os.path.exists(temp_file)
    
    # Verify file contains encrypted data
    with open(temp_file, 'rb') as f:
        stored_data = f.read()
    assert stored_data != json.dumps(data).encode()  # Should be encrypted
    
    # Load and decrypt
    loaded_data = storage.load()
    assert loaded_data == data

def test_encrypted_storage_with_compression(temp_file, security_manager):
    """Test encrypted JSON storage with compression."""
    storage = EncryptedJSONStorage(temp_file, security_manager, compression_level=6)
    
    # Create a larger dataset
    data = [
        {
            "id": i,
            "name": f"test_{i}",
            "description": "A" * 1000,  # 1KB of text
            "values": list(range(100))
        }
        for i in range(100)  # 100 documents
    ]
    
    # Save encrypted and compressed data
    storage.save(data)
    
    # Load and verify
    loaded_data = storage.load()
    assert loaded_data == data

def test_encryption_key_required():
    """Test that encryption key is required."""
    security = SecurityManager()  # No password set
    
    with pytest.raises(ValueError):
        security.encrypt("data")
    
    with pytest.raises(ValueError):
        security.decrypt(b"data")

def test_salt_persistence():
    """Test that salt can be reused for key derivation."""
    # Create first security manager
    security1 = SecurityManager(password="test_password")
    data = "test data"
    encrypted = security1.encrypt(data)
    
    # Create second security manager with same password and salt
    security2 = SecurityManager(password="test_password", salt=security1.salt)
    
    # Should be able to decrypt with second manager
    decrypted = security2.decrypt(encrypted)
    assert decrypted.decode() == data
