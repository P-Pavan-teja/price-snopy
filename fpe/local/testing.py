import pandas as pd
import io
from function import *
import sys
import datetime


param_file = "/Users/pavanteja/data_engineering/obfuscation/python_files/in_mac/parameters.param"
params = load_params(param_file)

log_file = params["log_file"]
output_file = params["output_file"]
dict_path = params["dict_path"]
encryption_key = params["encryption_key"]
decrypted_path = params["decrypted_path"]
source_path = params["source_path"]
source_sheet_path = params["source_sheet_path"]
output_file = params["output_file"]

# ---------------------------------------------------------------------------------------------
# Loging
# ---------------------------------------------------------------------------------------------
log_buffer = io.StringIO()
sys.stdout = logs(sys.stdout, log_buffer)
sys.stderr = sys.stdout

# df = pd.read_csv("/Users/pavanteja/data_engineering/obfuscation/sample_data/customer.csv")

# df = pd.ExcelFile("/Users/pavanteja/data_engineering/obfuscation/sample_data/customer.xlsx")
df = pd.read_excel(source_path, sheet_name=source_sheet_path)

# print(df)

print("=" * 80)
print("FORMAT-PRESERVING ENCRYPTION WITH CSV DATA DICTIONARY")
print("=" * 80)

# ---------------------------------------------------------------------------------------------
# Create sample data dictionary CSV
# ---------------------------------------------------------------------------------------------

sample_dict = pd.DataFrame({
    'field_name': ['social_security_number', 'credit_card', 'phone', 'email', 'account_number'],
    'type': ['numeric', 'numeric', 'numeric', 'alphanumeric', 'numeric'],
    'format': ['999-99-9999', '9999-9999-9999-9999', '(999) 999-9999', '', '99999999'],
    'description': ['Social Security Number', 'Credit Card Number', 'Phone Number',
                    'Email Address', 'Bank Account Number']
})
# ---------------------------------------------------------------------------------------------
# dict_path = f'{dictionary_files}'
# ---------------------------------------------------------------------------------------------

sample_dict.to_csv(dict_path, index=False)
print(f"\n✓ Sample data dictionary created: {dict_path}")

print("\nData Dictionary Contents:")
print("-" * 80)
# print(sample_dict.to_string(index=False))

# ---------------------------------------------------------------------------
# Load the actual key bytes from your saved .bin file
# ---------------------------------------------------------------------------
key = DataEncryptor.load_key(encryption_key)

# ---------------------------------------------------------------------------------------------
# Initialize encryptor with CSV data dictionary
# ---------------------------------------------------------------------------------------------
print("\n2. Loading Data Dictionary from CSV...")
print("-" * 80)
encryptor = DataEncryptor(key =key, data_dictionary_path=dict_path)

# ---------------------------------------------------------------------------------------------
# Display loaded sensitive fields
# ---------------------------------------------------------------------------------------------
print("\nLoaded Sensitive Fields:")
for field, config in encryptor.data_dictionary['sensitive_fields'].items():
    desc = config.get('description', 'N/A')
    field_type = config.get('type', 'N/A')
    field_format = config.get('format', 'N/A')
    print(f"  • {field:20s} | Type: {field_type:15s} | Format: {str(field_format):25s} | {desc}")


    # Save the key for later decryption in /Users/pavanteja/data_engineering/obfuscation/python_files/keys
# encryptor.save_key(encryption_key)

# Encrypt the dataframe
print("\n3. Encrypting Sensitive Fields...")
print("-" * 80)
encrypted_df = encryptor.encrypt_dataframe(df)

print("\n4. Encrypted Data:")
print("-" * 80)
# print(encrypted_df.head())

# Save encrypted data
encrypted_df.to_csv(output_file, index=False)
print(f"\n✓ Encrypted data saved to: {output_file}")

# Decrypt the dataframe
print("\n5. Decrypting Data...")
print("-" * 80)
decrypted_df = encryptor.decrypt_dataframe(encrypted_df)

print("\n6. Decrypted Data:")
print("-" * 80)
# print(decrypted_df.to_string(index=False))

# Verify decryption
print("\n7. Verification:")
print("-" * 80)
for col in df.columns:
    if encryptor.is_sensitive_field(col):
        match = (df[col] == decrypted_df[col]).all()
        status = '✓ SUCCESS' if match else '✗ FAILED'
        print(f"{col:20s} - Decryption {status}")

print("\n7. Re-Verification:")
print("-" * 80)

for col in df.columns:
    if encryptor.is_sensitive_field(col):
        left  = df[col].astype(str)
        right = decrypted_df[col].astype(str)
        match = (left == right).all()
        print(f"{col:20s} - Decryption {'✓ SUCCESS' if match else '✗ FAILED'}")

print("\n8. CSV File Encryption Workflow:")
print("-" * 80)

# decrypted_path_file_name = f"output_file_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
decrypted_path_file_name = "decrypted_file.csv"

# Save original data
decrypted_df.to_csv(f'{decrypted_path}/{decrypted_path_file_name}', index=False)
print(f"✓ Original data saved to: '{decrypted_path}/{decrypted_path_file_name}'")