from function import *
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
