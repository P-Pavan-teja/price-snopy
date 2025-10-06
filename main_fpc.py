from Crypto.Hash import HMAC, SHA256
from Crypto.Random import get_random_bytes
import hashlib
import pandas as pd
import json
import os
from typing import Dict, List, Any


class FormatPreservingEncryption:
    def __init__(self, key):
        """
        Initialize FPE with a secret key
        :param key: bytes - encryption key (16 bytes recommended)
        """
        self.key = key
        self.rounds = 10

    def encrypt_numeric(self, plaintext, format_template):
        """
        Encrypt numeric data while preserving format
        :param plaintext: str - data to encrypt (e.g., "123-45-6789")
        :param format_template: str - format pattern (e.g., "999-99-9999")
        :return: str - encrypted data in same format
        """
        if not plaintext or pd.isna(plaintext):
            return plaintext

        plaintext = str(plaintext)
        # Extract only digits
        digits = ''.join(c for c in plaintext if c.isdigit())
        if not digits:
            return plaintext

        n = len(digits)

        # Convert to number array
        num_array = [int(d) for d in digits]

        # Feistel network encryption
        for round_num in range(self.rounds):
            mid = n // 2
            left = num_array[:mid]
            right = num_array[mid:]

            # Round function
            round_key = self._get_round_key(round_num)
            f = self._round_function(right, round_key, len(left))

            # XOR left with f(right)
            new_left = [(d + f[i]) % 10 for i, d in enumerate(left)]

            # Swap
            num_array = right + new_left

        # Reconstruct with original format
        return self._apply_format(''.join(map(str, num_array)), plaintext)

    def decrypt_numeric(self, ciphertext, format_template):
        """
        Decrypt numeric data
        :param ciphertext: str - encrypted data
        :param format_template: str - format pattern
        :return: str - decrypted data in original format
        """
        if not ciphertext or pd.isna(ciphertext):
            return ciphertext

        ciphertext = str(ciphertext)
        # Extract only digits
        digits = ''.join(c for c in ciphertext if c.isdigit())
        if not digits:
            return ciphertext

        n = len(digits)
        num_array = [int(d) for d in digits]

        # Reverse Feistel network
        for round_num in range(self.rounds - 1, -1, -1):
            mid = n // 2
            right = num_array[:n - mid]
            left = num_array[n - mid:]

            round_key = self._get_round_key(round_num)
            f = self._round_function(right, round_key, len(left))

            new_left = [(d - f[i] + 10) % 10 for i, d in enumerate(left)]
            num_array = new_left + right

        return self._apply_format(''.join(map(str, num_array)), ciphertext)

    def encrypt_alphanumeric(self, plaintext):
        """
        Encrypt alphanumeric string while preserving character types
        :param plaintext: str - data to encrypt
        :return: str - encrypted data
        """
        if not plaintext or pd.isna(plaintext):
            return plaintext

        plaintext = str(plaintext)
        result = []
        for i, char in enumerate(plaintext):
            if 'A' <= char <= 'Z':
                hash_val = self._hmac_hash((str(char) + str(i)).encode())
                shift = int(hash_val[:2], 16) % 26
                result.append(chr(65 + (ord(char) - 65 + shift) % 26))
            elif 'a' <= char <= 'z':
                hash_val = self._hmac_hash((str(char) + str(i)).encode())
                shift = int(hash_val[:2], 16) % 26
                result.append(chr(97 + (ord(char) - 97 + shift) % 26))
            elif '0' <= char <= '9':
                hash_val = self._hmac_hash((str(char) + str(i)).encode())
                shift = int(hash_val[:2], 16) % 10
                result.append(str((int(char) + shift) % 10))
            else:
                result.append(char)
        return ''.join(result)

    def decrypt_alphanumeric(self, ciphertext):
        """
        Decrypt alphanumeric string
        :param ciphertext: str - encrypted data
        :return: str - decrypted data
        """
        if not ciphertext or pd.isna(ciphertext):
            return ciphertext

        ciphertext = str(ciphertext)
        result = []
        for i, char in enumerate(ciphertext):
            if 'A' <= char <= 'Z':
                hash_val = self._hmac_hash((str(ciphertext[i]) + str(i)).encode())
                shift = int(hash_val[:2], 16) % 26
                original_pos = (ord(char) - 65 - shift) % 26
                result.append(chr(65 + original_pos))
            elif 'a' <= char <= 'z':
                hash_val = self._hmac_hash((str(ciphertext[i]) + str(i)).encode())
                shift = int(hash_val[:2], 16) % 26
                original_pos = (ord(char) - 97 - shift) % 26
                result.append(chr(97 + original_pos))
            elif '0' <= char <= '9':
                hash_val = self._hmac_hash((str(ciphertext[i]) + str(i)).encode())
                shift = int(hash_val[:2], 16) % 10
                original_digit = (int(char) - shift) % 10
                result.append(str(original_digit))
            else:
                result.append(char)
        return ''.join(result)

    def _get_round_key(self, round_num):
        """Generate round-specific key"""
        combined = str(self.key.hex()) + str(round_num)
        return hashlib.sha256(combined.encode()).hexdigest()

    def _round_function(self, input_array, round_key, output_length):
        """Feistel round function"""
        input_str = ''.join(map(str, input_array))
        hmac_obj = HMAC.new(round_key.encode(), input_str.encode(), SHA256)
        hash_val = hmac_obj.hexdigest()

        result = []
        for i in range(output_length):
            byte_val = int(hash_val[i * 2:i * 2 + 2], 16)
            result.append(byte_val % 10)
        return result

    def _hmac_hash(self, data):
        """Generate HMAC hash"""
        hmac_obj = HMAC.new(self.key, data, SHA256)
        return hmac_obj.hexdigest()

    def _apply_format(self, digits, original):
        """Apply original format to digits"""
        result = []
        digit_index = 0
        for char in original:
            if char.isdigit():
                if digit_index < len(digits):
                    result.append(digits[digit_index])
                    digit_index += 1
                else:
                    result.append('0')
            else:
                result.append(char)
        return ''.join(result)


class DataEncryptor:
    def __init__(self, key=None, data_dictionary_path=None):
        """
        Initialize Data Encryptor with optional key and data dictionary
        :param key: bytes - encryption key (if None, generates new key)
        :param data_dictionary_path: str - path to data dictionary CSV file
        """
        self.key = key if key else get_random_bytes(16)
        self.fpe = FormatPreservingEncryption(self.key)
        self.data_dictionary = self._load_data_dictionary(data_dictionary_path)

    def _load_data_dictionary(self, path):
        """
        Load data dictionary from CSV file
        Expected CSV format:
        field_name,type,format,description
        ssn,numeric,999-99-9999,Social Security Number

        :param path: str - path to CSV file
        :return: dict - data dictionary
        """
        if path and os.path.exists(path):
            try:
                df_dict = pd.read_csv(path)
                sensitive_fields = {}

                for _, row in df_dict.iterrows():
                    field_name = str(row['field_name']).lower().strip()
                    field_config = {
                        "type": str(row['type']).lower().strip()
                    }

                    # Add format if provided
                    if 'format' in row and pd.notna(row['format']):
                        field_config["format"] = str(row['format']).strip()

                    # Add description if provided
                    if 'description' in row and pd.notna(row['description']):
                        field_config["description"] = str(row['description']).strip()

                    sensitive_fields[field_name] = field_config

                return {"sensitive_fields": sensitive_fields}
            except Exception as e:
                print(f"Warning: Could not load data dictionary from {path}: {e}")
                print("Using default data dictionary instead.")
                return self._get_default_data_dictionary()
        else:
            return self._get_default_data_dictionary()

    def _get_default_data_dictionary(self):
        """
        Get default data dictionary
        :return: dict - default data dictionary
        """
        return {
            "sensitive_fields": {
                "ssn": {"type": "numeric", "format": "999-99-9999", "description": "Social Security Number"},
                "social_security_number": {"type": "numeric", "format": "999-99-9999", "description": "Social Security Number"},
                "credit_card": {"type": "numeric", "format": "9999-9999-9999-9999", "description": "Credit Card Number"},
                "credit_card_number": {"type": "numeric", "format": "9999-9999-9999-9999", "description": "Credit Card Number"},
                "phone": {"type": "numeric", "format": "(999) 999-9999", "description": "Phone Number"},
                "phone_number": {"type": "numeric", "format": "(999) 999-9999", "description": "Phone Number"},
                "email": {"type": "alphanumeric", "description": "Email Address"},
                "customer_id": {"type": "alphanumeric", "description": "Customer ID"},
                "account_number": {"type": "numeric", "description": "Account Number"},
                "routing_number": {"type": "numeric", "description": "Routing Number"},
                "passport": {"type": "alphanumeric", "description": "Passport Number"},
                "drivers_license": {"type": "alphanumeric", "description": "Driver's License"},
                "date_of_birth": {"type": "numeric", "format": "99/99/9999", "description": "Date of Birth"},
                "dob": {"type": "numeric", "format": "99/99/9999", "description": "Date of Birth"}
            }
        }

    def is_sensitive_field(self, field_name):
        """
        Check if a field is marked as sensitive in data dictionary
        :param field_name: str - field name to check
        :return: bool - True if sensitive
        """
        field_lower = field_name.lower().strip()
        return field_lower in self.data_dictionary["sensitive_fields"]

    def get_field_config(self, field_name):
        """
        Get encryption configuration for a field
        :param field_name: str - field name
        :return: dict - field configuration
        """
        field_lower = field_name.lower().strip()
        return self.data_dictionary["sensitive_fields"].get(field_lower, {})

    def encrypt_value(self, value, field_config):
        """
        Encrypt a single value based on field configuration
        :param value: Any - value to encrypt
        :param field_config: dict - field configuration
        :return: Any - encrypted value
        """
        if pd.isna(value) or value == '':
            return value

        field_type = field_config.get("type", "alphanumeric")

        if field_type == "numeric":
            format_template = field_config.get("format", None)
            return self.fpe.encrypt_numeric(str(value), format_template)
        elif field_type == "alphanumeric":
            return self.fpe.encrypt_alphanumeric(str(value))
        else:
            return value

    def decrypt_value(self, value, field_config):
        """
        Decrypt a single value based on field configuration
        :param value: Any - value to decrypt
        :param field_config: dict - field configuration
        :return: Any - decrypted value
        """
        if pd.isna(value) or value == '':
            return value

        field_type = field_config.get("type", "alphanumeric")

        if field_type == "numeric":
            format_template = field_config.get("format", None)
            return self.fpe.decrypt_numeric(str(value), format_template)
        elif field_type == "alphanumeric":
            return self.fpe.decrypt_alphanumeric(str(value))
        else:
            return value

    def encrypt_dataframe(self, df, inplace=False):
        """
        Encrypt sensitive fields in a DataFrame
        :param df: pd.DataFrame - dataframe to encrypt
        :param inplace: bool - modify original dataframe
        :return: pd.DataFrame - encrypted dataframe
        """
        if not inplace:
            df = df.copy()

        for column in df.columns:
            if self.is_sensitive_field(column):
                field_config = self.get_field_config(column)
                print(f"Encrypting column: {column} (type: {field_config.get('type')})")
                df[column] = df[column].apply(lambda x: self.encrypt_value(x, field_config))

        return df

    def decrypt_dataframe(self, df, inplace=False):
        """
        Decrypt sensitive fields in a DataFrame
        :param df: pd.DataFrame - dataframe to decrypt
        :param inplace: bool - modify original dataframe
        :return: pd.DataFrame - decrypted dataframe
        """
        if not inplace:
            df = df.copy()

        for column in df.columns:
            if self.is_sensitive_field(column):
                field_config = self.get_field_config(column)
                print(f"Decrypting column: {column} (type: {field_config.get('type')})")
                df[column] = df[column].apply(lambda x: self.decrypt_value(x, field_config))

        return df

    def encrypt_csv(self, input_path, output_path):
        """
        Encrypt sensitive fields in a CSV file
        :param input_path: str - path to input CSV
        :param output_path: str - path to output encrypted CSV
        """
        print(f"Reading CSV from: {input_path}")
        df = pd.read_csv(input_path)

        print(f"Total rows: {len(df)}")
        print(f"Columns: {list(df.columns)}")

        encrypted_df = self.encrypt_dataframe(df)

        encrypted_df.to_csv(output_path, index=False)
        print(f"\nEncrypted data saved to: {output_path}")

        return encrypted_df

    def decrypt_csv(self, input_path, output_path):
        """
        Decrypt sensitive fields in a CSV file
        :param input_path: str - path to encrypted CSV
        :param output_path: str - path to output decrypted CSV
        """
        print(f"Reading encrypted CSV from: {input_path}")
        df = pd.read_csv(input_path)

        print(f"Total rows: {len(df)}")
        print(f"Columns: {list(df.columns)}")

        decrypted_df = self.decrypt_dataframe(df)

        decrypted_df.to_csv(output_path, index=False)
        print(f"\nDecrypted data saved to: {output_path}")

        return decrypted_df

    def save_key(self, key_path):
        """
        Save encryption key to file
        :param key_path: str - path to save key
        """
        with open(key_path, 'wb') as f:
            f.write(self.key)
        print(f"Encryption key saved to: {key_path}")

    @staticmethod
    def load_key(key_path):
        """
        Load encryption key from file
        :param key_path: str - path to key file
        :return: bytes - encryption key
        """
        with open(key_path, 'rb') as f:
            return f.read()


# Example Usage
if __name__ == "__main__":
    print("=" * 80)
    print("FORMAT-PRESERVING ENCRYPTION WITH CSV DATA DICTIONARY")
    print("=" * 80)

    # Create sample data dictionary CSV
    sample_dict = pd.DataFrame({
        'field_name': ['ssn', 'credit_card', 'phone', 'email', 'customer_id', 'account_number'],
        'type': ['numeric', 'numeric', 'numeric', 'alphanumeric', 'alphanumeric', 'numeric'],
        'format': ['999-99-9999', '9999-9999-9999-9999', '(999) 999-9999', None, None, '99999999'],
        'description': ['Social Security Number', 'Credit Card Number', 'Phone Number',
                        'Email Address', 'Customer Identifier', 'Bank Account Number']
    })

    # Save sample data dictionary
    dict_path = 'data_dictionary.csv'
    sample_dict.to_csv(dict_path, index=False)
    print(f"\n✓ Sample data dictionary created: {dict_path}")

    print("\nData Dictionary Contents:")
    print("-" * 80)
    print(sample_dict.to_string(index=False))

    # Create sample data
    sample_data = pd.DataFrame({
        'customer_id': ['CUST12345', 'CUST67890', 'CUST11111'],
        'name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'ssn': ['123-45-6789', '987-65-4321', '555-55-5555'],
        'credit_card': ['4111-1111-1111-1111', '5500-0000-0000-0004', '3782-8224-6310-005'],
        'email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
        'phone': ['(555) 123-4567', '(555) 987-6543', '(555) 555-5555'],
        'account_number': ['12345678', '87654321', '11111111'],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Rd']
    })

    print("\n1. Original Data:")
    print("-" * 80)
    print(sample_data.to_string(index=False))

    # Initialize encryptor with CSV data dictionary
    print("\n2. Loading Data Dictionary from CSV...")
    print("-" * 80)
    encryptor = DataEncryptor(data_dictionary_path=dict_path)

    # Display loaded sensitive fields
    print("\nLoaded Sensitive Fields:")
    for field, config in encryptor.data_dictionary['sensitive_fields'].items():
        desc = config.get('description', 'N/A')
        field_type = config.get('type', 'N/A')
        field_format = config.get('format', 'N/A')
        print(f"  • {field:20s} | Type: {field_type:15s} | Format: {str(field_format):25s} | {desc}")

    # Save the key for later decryption
    encryptor.save_key('encryption_key.bin')

    # Encrypt the dataframe
    print("\n3. Encrypting Sensitive Fields...")
    print("-" * 80)
    encrypted_df = encryptor.encrypt_dataframe(sample_data)

    print("\n4. Encrypted Data:")
    print("-" * 80)
    print(encrypted_df.to_string(index=False))

    # Save encrypted data
    encrypted_df.to_csv('encrypted_data.csv', index=False)
    print("\n✓ Encrypted data saved to: encrypted_data.csv")

    # Decrypt the dataframe
    print("\n5. Decrypting Data...")
    print("-" * 80)
    decrypted_df = encryptor.decrypt_dataframe(encrypted_df)

    print("\n6. Decrypted Data:")
    print("-" * 80)
    print(decrypted_df.to_string(index=False))

    # Verify decryption
    print("\n7. Verification:")
    print("-" * 80)
    for col in sample_data.columns:
        if encryptor.is_sensitive_field(col):
            match = (sample_data[col] == decrypted_df[col]).all()
            status = '✓ SUCCESS' if match else '✗ FAILED'
            print(f"{col:20s} - Decryption {status}")

    # Demonstrate CSV encryption workflow
    print("\n8. CSV File Encryption Workflow:")
    print("-" * 80)

    # Save original data
    sample_data.to_csv('original_data.csv', index=False)
    print("✓ Original data saved to: original_data.csv")

    # Encrypt CSV file
    encryptor.encrypt_csv('original_data.csv', 'encrypted_from_csv.csv')

    # Load existing key and decrypt
    print("\n9. Decryption Using Saved Key:")
    print("-" * 80)
    loaded_key = DataEncryptor.load_key('encryption_key.bin')
    decryptor = DataEncryptor(key=loaded_key, data_dictionary_path=dict_path)
    decryptor.decrypt_csv('encrypted_from_csv.csv', 'decrypted_data.csv')

    print("\n" + "=" * 80)
    print("FILES CREATED:")
    print("=" * 80)
    print("  1. data_dictionary.csv       - CSV data dictionary (sensitive field definitions)")
    print("  2. encryption_key.bin        - Encryption key (KEEP SECURE!)")
    print("  3. original_data.csv         - Original unencrypted data")
    print("  4. encrypted_data.csv        - Encrypted data (from DataFrame)")
    print("  5. encrypted_from_csv.csv    - Encrypted data (from CSV file)")
    print("  6. decrypted_data.csv        - Decrypted data (verification)")
    print("\n⚠️  IMPORTANT: Keep 'encryption_key.bin' and 'data_dictionary.csv' secure!")
    print("=" * 80)

    print("\n" + "=" * 80)
    print("HOW TO USE WITH YOUR OWN DATA:")
    print("=" * 80)
    print("""
1. Create your data dictionary CSV with columns: field_name, type, format, description
   Example:
   field_name,type,format,description
   ssn,numeric,999-99-9999,Social Security Number
   email,alphanumeric,,Email Address
2. Initialize encryptor with your dictionary:
   encryptor = DataEncryptor(data_dictionary_path='your_dictionary.csv')
3. Encrypt your data:
   encryptor.encrypt_csv('your_input.csv', 'your_output.csv')
4. Save the encryption key:
   encryptor.save_key('your_key.bin')
5. To decrypt later:
   key = DataEncryptor.load_key('your_key.bin')
   decryptor = DataEncryptor(key=key, data_dictionary_path='your_dictionary.csv')
   decryptor.decrypt_csv('your_output.csv', 'decrypted.csv')
""")
