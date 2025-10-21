# use_key_encrypt.py
from function import *   # your existing class

# Define your project key (must be bytes)
key = b'pavanteja'  # ✅ 16 bytes

encryption_key = '/Users/pavanteja/data_engineering/obfuscation/python_files/keys/custom_project_key.bin'

# Create encryptor with that key
encryptor = DataEncryptor(key=key)

# Save it once for reuse
encryptor.save_key(encryption_key)

print(f"✓ Project key saved as {encryption_key}")