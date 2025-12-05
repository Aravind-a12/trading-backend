from cryptography.fernet import Fernet
import base64
import os
from dotenv import load_dotenv

load_dotenv()

# Get encryption key from environment or generate one
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
if not ENCRYPTION_KEY:
    # Generate a new key if none exists
    ENCRYPTION_KEY = Fernet.generate_key().decode()
    print(f"Generated new encryption key: {ENCRYPTION_KEY}")
    print("Please add this to your .env file as ENCRYPTION_KEY")

# Initialize Fernet cipher
cipher = Fernet(ENCRYPTION_KEY.encode())

def encrypt_data(data: str) -> str:
    """Encrypt sensitive data like API keys."""
    if not data:
        return data
    encrypted_data = cipher.encrypt(data.encode())
    return base64.b64encode(encrypted_data).decode()

def decrypt_data(encrypted_data: str) -> str:
    """Decrypt sensitive data like API keys."""
    if not encrypted_data:
        return encrypted_data
    try:
        decoded_data = base64.b64decode(encrypted_data.encode())
        decrypted_data = cipher.decrypt(decoded_data)
        return decrypted_data.decode()
    except Exception as e:
        print(f"Error decrypting data: {e}")
        return ""

def generate_encryption_key() -> str:
    """Generate a new encryption key."""
    return Fernet.generate_key().decode()
