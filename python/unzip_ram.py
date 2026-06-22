import sys
import io
import os
import zipfile
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3


SPLIT_SIZE = 250 * 1024 * 1024
READ_BLOCK = 16 * 1024 * 1024
MAX_THREADS = min((os.cpu_count() or 4) * 2, 12)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

s3 = boto3.client("s3")


def get_zip_keys(bucket, source_folder, zip_input):
    zip_keys = []

    if zip_input == "ALL":
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=source_folder + "/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.lower().endswith(".zip"):
                    zip_keys.append(key)
    else:
        for name in zip_input.split(","):
            name = name.strip()
            if name:
                zip_keys.append(source_folder + "/" + name)

    return zip_keys


def get_csv_files(zip_bytes):
    csv_files = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for m in zf.infolist():
            name = m.filename.replace("\\", "/").strip("/")
            base = os.path.basename(name)

            if (
                name
                and not name.endswith("/")
                and not name.startswith("__MACOSX/")
                and not base.startswith("._")
                and name.lower().endswith(".csv")
            ):
                csv_files.append(m)

    return csv_files


def upload_small_file(bucket, target_folder, zip_bytes, zip_name, member_name):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        data = zf.read(member_name)

    clean_name = member_name.replace("\\", "/").strip("/")
    out_key = f"{target_folder}/{zip_name}/{clean_name}"

    s3.upload_fileobj(io.BytesIO(data), bucket, out_key)
    log.info("Uploaded small file: %s", out_key)


def upload_small_files(bucket, target_folder, zip_bytes, zip_name, small_files):
    log.info("Phase 1: uploading small files in parallel")

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        futures = []

        for m in small_files:
            futures.append(
                pool.submit(upload_small_file, bucket, target_folder, zip_bytes, zip_name, m.filename)
            )

        for future in as_completed(futures):
            future.result()


def split_large_file(bucket, target_folder, zip_bytes, zip_name, member_name, has_header):
    clean_name = member_name.replace("\\", "/").strip("/")
    file_base = os.path.splitext(os.path.basename(clean_name))[0]
    out_dir = f"{target_folder}/{zip_name}/{file_base}"

    header = b""
    leftover = b""
    current_part = bytearray()
    part_number = 1
    futures = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        with zf.open(member_name) as stream:
            if has_header:
                first = bytearray()
                while b"\n" not in first:
                    chunk = stream.read(READ_BLOCK)
                    if not chunk:
                        break
                    first.extend(chunk)

                pos = first.find(b"\n")
                if pos != -1:
                    header = bytes(first[:pos + 1])
                    leftover = bytes(first[pos + 1:])
                else:
                    header = bytes(first)
                    leftover = b""

            with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
                while True:
                    block = stream.read(READ_BLOCK)
                    data = leftover + block
                    leftover = b""

                    if not block:
                        current_part.extend(data)
                        break

                    pos = data.rfind(b"\n")
                    if pos == -1:
                        leftover = data
                        continue

                    leftover = data[pos + 1:]
                    data = data[:pos + 1]
                    current_part.extend(data)

                    if len(current_part) >= SPLIT_SIZE:
                        out_key = f"{out_dir}/{file_base}_part{str(part_number).zfill(3)}.csv"
                        payload = header + bytes(current_part)
                        futures.append(pool.submit(s3.upload_fileobj, io.BytesIO(payload), bucket, out_key))
                        log.info("Uploading split file: %s", out_key)
                        part_number += 1
                        current_part = bytearray()

                if leftover:
                    current_part.extend(leftover)

                if current_part:
                    out_key = f"{out_dir}/{file_base}_part{str(part_number).zfill(3)}.csv"
                    payload = header + bytes(current_part)
                    futures.append(pool.submit(s3.upload_fileobj, io.BytesIO(payload), bucket, out_key))
                    log.info("Uploading split file: %s", out_key)

                for future in as_completed(futures):
                    future.result()


def process_zip_file(bucket, target_folder, zip_key, has_header):
    log.info("Processing ZIP: %s", zip_key)

    zip_bytes = s3.get_object(Bucket=bucket, Key=zip_key)["Body"].read()
    zip_name = os.path.splitext(os.path.basename(zip_key))[0]

    csv_files = get_csv_files(zip_bytes)
    small_files = [m for m in csv_files if m.file_size < SPLIT_SIZE]
    large_files = [m for m in csv_files if m.file_size >= SPLIT_SIZE]

    log.info("Small files: %s | Large files: %s", len(small_files), len(large_files))

    if small_files:
        upload_small_files(bucket, target_folder, zip_bytes, zip_name, small_files)

    if large_files:
        log.info("Phase 2: processing large files one by one")
        for m in large_files:
            log.info("Splitting file: %s", m.filename)
            split_large_file(bucket, target_folder, zip_bytes, zip_name, m.filename, has_header)

    del zip_bytes


def main():
    if len(sys.argv) != 6:
        print("Usage:")
        print("python3 unzip_s3_csv.py <s3_bucket> <s3_source_files_location> <s3_target_files_location> <zip_file_name> <has_header>")
        print("zip_file_name = one zip | comma separated zips | ALL")
        sys.exit(1)

    bucket = sys.argv[1]
    source_folder = sys.argv[2].strip("/")
    target_folder = sys.argv[3].strip("/")
    zip_input = sys.argv[4].strip()
    has_header = sys.argv[5].strip().lower() in ["true", "1", "yes", "y"]

    try:
        zip_keys = get_zip_keys(bucket, source_folder, zip_input)

        if not zip_keys:
            log.info("No zip files found")
            return

        for zip_key in zip_keys:
            process_zip_file(bucket, target_folder, zip_key, has_header)

        log.info("Done")

    except Exception:
        log.exception("Job failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
