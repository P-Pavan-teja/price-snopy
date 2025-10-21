import boto3
import pandas as pd
import io
import datetime
import sys
import traceback

s3 = boto3.client("s3")
from function import *

# s3://fpe-source-target-dict-files/srcfiles/customer.xlsx

# ------------------------------------------------------------------
# input arguments
# ------------------------------------------------------------------

# s3://fpe-source-target-dict-files/paramfiles/parameter_s3.param

if __name__ == "__main__":
    try:
        param_file = "/Users/pavanteja/data_engineering/obfuscation/python_files/s3_working/parameter_s3.param"
        params = load_params(param_file)

        bucket = params["bucket"]
        source_key = params["source_key"]
        dict_key = params["dict_key"]
        enc_bucket = params["enc_bucket"]
        enc_s3_key = params["enc_s3_key"]
        output_key = params["output_key"]
        log_key = params["log_key"]
        # ------------------------------------------------------------------
        # loging
        # ------------------------------------------------------------------

        log_buffer = io.StringIO()
        sys.stdout = logs(sys.stdout, log_buffer)
        sys.stderr = sys.stdout

        # ------------------------------------------------------------------
        #FORMAT-PRESERVING ENCRYPTION WITH CSV DATA DICTIONARY
        # ------------------------------------------------------------------
        print("=" * 80)
        print("FORMAT-PRESERVING ENCRYPTION WITH CSV DATA DICTIONARY")
        print("=" * 80)

        # ------------------------------------------------------------------
        # Getting srcfiles ,dictionary and encrypted_key file
        # ------------------------------------------------------------------
        print("=" * 80)
        print("\n1. Getting srcfiles ,dictionary and encrypted_key files from S3")
        print("=" * 80)

        # getting Source file
        src_s3_file = s3.get_object(Bucket = bucket, Key = source_key)['Body'].read()
        src_df = pd.read_excel(io.BytesIO(src_s3_file), sheet_name="sample_2")

        print("Fetching source file is successfull!")
        # print(src_df.head())

        # getting encripted key
        encryption_key = s3.get_object(Bucket = enc_bucket, Key = enc_s3_key)['Body'].read()
        print("Fetching Encription key is successfull!")
        # print(encryption_key)

        # getting dictionary file
        dict_s3_file = s3.get_object(Bucket = bucket, Key = dict_key)['Body'].read()
        dict_df = pd.read_csv(io.BytesIO(dict_s3_file))

        print("Fetching dictionary is successfull!")
        # print(dict_df)


        # ------------------------------------------------------------------
        # Initialize encryptor with CSV data dictionary
        # ------------------------------------------------------------------
        print("\n2. Loading Data Dictionary from CSV...")
        print("-" * 80)


        dict_path = f's3://{bucket}/{dict_key}'
        print(dict_path)


        encryptor = DataEncryptor(key = encryption_key, data_dictionary_path = dict_path)
        print("\nLoaded Sensitive Fields:")
        for field, config in encryptor.data_dictionary['sensitive_fields'].items():
            desc = config.get('description', 'N/A')
            field_type = config.get('type', 'N/A')
            field_format = config.get('format', 'N/A')
            print(f"  • {field:20s} | Type: {field_type:15s} | Format: {str(field_format):25s} | {desc}")

        # ------------------------------------------------------------------
        # Encrypt the dataframe
        # ------------------------------------------------------------------
        print("\n3. Encrypting Sensitive Fields...")
        print("-" * 80)
        encrypted_df = encryptor.encrypt_dataframe(src_df)

        print("Data Encryption Completed Data")
        print("-" * 80)
        # print(encrypted_df.head())


        # ------------------------------------------------------------------
        # Saving Encrypted data file to s3
        # ------------------------------------------------------------------
        print("\n4. Saving Encrypted data file to s3")
        csv_bytes = io.StringIO()
        encrypted_df.to_csv(csv_bytes, index=False)

        output_file_name = f"output_file_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"


        s3.put_object(Bucket=bucket, Key=f'{output_key}{output_file_name}', Body=csv_bytes.getvalue())
        print(f"Encrypted files saved in s3://{bucket}/{output_key}{output_file_name} Completed!")

        # ------------------------------------------------------------------
        # validating data by decrypting
        # ------------------------------------------------------------------
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
        for col in src_df.columns:
            if encryptor.is_sensitive_field(col):
                match = (src_df[col] == decrypted_df[col]).all()
                status = '✓ SUCCESS' if match else '✗ FAILED'
                print(f"{col:20s} - Decryption {status}")

        print("\n7. Re-Verification:")
        print("-" * 80)

        for col in src_df.columns:
            if encryptor.is_sensitive_field(col):
                left  = src_df[col].astype(str)
                right = decrypted_df[col].astype(str)
                match = (left == right).all()
                print(f"{col:20s} - Decryption {'✓ SUCCESS' if match else '✗ FAILED'}")

        # print("\n8. CSV File Encryption Workflow:")
        # print("-" * 80)

        # # decrypted_path_file_name = f"output_file_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        # decrypted_path_file_name = "decrypted_file.csv"

        # # Save original data
        # decrypted_df.to_csv(f'{decrypted_path}/{decrypted_path_file_name}', index=False)
        # print(f"✓ Original data saved to: '{decrypted_path}/{decrypted_path_file_name}'")

        # ------------------------------------------------------------------
        # Saving log file to s3
        # ------------------------------------------------------------------

        # log_content = log_buffer.getvalue()

        # log_file = f"{log_key}log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

        # s3.put_object(Bucket=bucket, Key=log_file,Body=log_content.encode("utf-8"))
        # print("Log uploaded to S3")

    except Exception as e:
        traceback.print_exc()

    finally:
        # Always save/upload logs, even if error
        print("\nUploading log to S3...")
        log_content = log_buffer.getvalue()
        log_file = f"{log_key}log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        s3.put_object(Bucket=bucket, Key=log_file, Body=log_content.encode("utf-8"))
        print(f"Log uploaded to s3://{bucket}/{log_file}")
