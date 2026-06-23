import sys
import io
import os
import json
import csv
import zipfile
import shutil
import logging
import tempfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

s3 = boto3.client("s3")


def now_ts():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def today_folder():
    return datetime.now().strftime("%Y%m%d")


def normalize_prefix(value):
    return str(value).strip().strip("/")


def count_total_rows_bytes(data_bytes):
    if not data_bytes:
        return 0
    return data_bytes.count(b"\n") + (0 if data_bytes.endswith(b"\n") else 1)


def make_csv_bytes(headers, rows):
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(headers)
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def upload_count_file(bucket, key, headers, rows):
    payload = make_csv_bytes(headers, rows)
    s3.put_object(Bucket=bucket, Key=key, Body=payload, ContentType="text/csv")


def move_s3_object(bucket, source_key, target_base_prefix):
    base_prefix = normalize_prefix(target_base_prefix)
    dated_prefix = f"{base_prefix}/{today_folder()}"
    target_key = f"{dated_prefix}/{os.path.basename(source_key)}"

    log.info("Moving zip from s3://%s/%s to s3://%s/%s", bucket, source_key, bucket, target_key)
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=target_key
    )
    s3.delete_object(Bucket=bucket, Key=source_key)
    return target_key


def load_params_from_s3(bucket, param_key):
    response = s3.get_object(Bucket=bucket, Key=param_key)
    content = response["Body"].read().decode("utf-8")
    return json.loads(content)


def get_zip_keys(bucket, source_folder, zip_input):
    zip_keys = []

    if str(zip_input).strip().upper() == "ALL":
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=source_folder.rstrip("/") + "/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.lower().endswith(".zip"):
                    zip_keys.append(key)
    else:
        for name in str(zip_input).split(","):
            name = name.strip()
            if name:
                zip_keys.append(source_folder.rstrip("/") + "/" + name)

    return zip_keys


def get_csv_files(local_zip_path):
    csv_files = []

    with zipfile.ZipFile(local_zip_path) as zf:
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


def upload_small_file(bucket, target_folder, local_zip_path, zip_name, member_name):
    with zipfile.ZipFile(local_zip_path) as zf:
        with zf.open(member_name) as src:
            data = src.read()

    clean_name = member_name.replace("\\", "/").strip("/")
    out_key = f"{target_folder.rstrip('/')}/{zip_name}/{clean_name}"

    s3.upload_fileobj(io.BytesIO(data), bucket, out_key)
    row_count = count_total_rows_bytes(data)
    log.info("Uploaded small file: %s", out_key)

    return {
        "source_full_csv_file_name": clean_name,
        "target_full_csv_file_name": out_key,
        "row_count": row_count,
        "is_split": False,
        "split_files_count": 1
    }


def upload_small_files(bucket, target_folder, local_zip_path, zip_name, small_files, max_threads):
    results = []
    log.info("Phase 1: uploading small files in parallel")

    with ThreadPoolExecutor(max_workers=max_threads) as pool:
        futures = [
            pool.submit(upload_small_file, bucket, target_folder, local_zip_path, zip_name, m.filename)
            for m in small_files
        ]

        for future in as_completed(futures):
            results.append(future.result())

    return results


def split_large_file(bucket, target_folder, local_zip_path, zip_name, member_name, has_header, split_size, read_block, max_threads):
    clean_name = member_name.replace("\\", "/").strip("/")
    file_base = os.path.splitext(os.path.basename(clean_name))[0]
    out_dir = f"{target_folder.rstrip('/')}/{zip_name}/{file_base}"

    processed_rows = []
    header = b""
    leftover = b""
    current_part = bytearray()
    part_number = 1
    split_files_count = 0

    with zipfile.ZipFile(local_zip_path) as zf:
        with zf.open(member_name) as src:
            source_bytes = src.read()
    source_row_count = count_total_rows_bytes(source_bytes)

    with zipfile.ZipFile(local_zip_path) as zf:
        with zf.open(member_name) as stream:
            if has_header:
                first = bytearray()
                while b"\n" not in first:
                    chunk = stream.read(read_block)
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

            futures = []
            with ThreadPoolExecutor(max_workers=max_threads) as pool:
                while True:
                    block = stream.read(read_block)
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

                    if len(current_part) >= split_size:
                        out_key = f"{out_dir}/{file_base}_part{str(part_number).zfill(3)}.csv"
                        payload = header + bytes(current_part)
                        row_count = count_total_rows_bytes(payload)
                        futures.append(pool.submit(s3.upload_fileobj, io.BytesIO(payload), bucket, out_key))
                        processed_rows.append({
                            "source_full_csv_file_name": clean_name,
                            "target_full_csv_file_name": out_key,
                            "row_count": row_count,
                            "is_split": True
                        })
                        log.info("Uploading split file: %s", out_key)
                        split_files_count += 1
                        part_number += 1
                        current_part = bytearray()

                if leftover:
                    current_part.extend(leftover)

                if current_part:
                    out_key = f"{out_dir}/{file_base}_part{str(part_number).zfill(3)}.csv"
                    payload = header + bytes(current_part)
                    row_count = count_total_rows_bytes(payload)
                    futures.append(pool.submit(s3.upload_fileobj, io.BytesIO(payload), bucket, out_key))
                    processed_rows.append({
                        "source_full_csv_file_name": clean_name,
                        "target_full_csv_file_name": out_key,
                        "row_count": row_count,
                        "is_split": True
                    })
                    log.info("Uploading split file: %s", out_key)
                    split_files_count += 1

                for future in as_completed(futures):
                    future.result()

    for row in processed_rows:
        row["split_files_count"] = split_files_count

    return {
        "source_count_row": {
            "source_full_csv_file_name": clean_name,
            "row_count": source_row_count,
            "is_split": True
        },
        "processed_count_rows": processed_rows
    }


def process_zip_file(bucket, target_folder, count_location, zip_key, has_header, split_size, read_block, max_threads):
    log.info("Processing ZIP: %s", zip_key)

    zip_name = os.path.splitext(os.path.basename(zip_key))[0]
    temp_dir = tempfile.mkdtemp(prefix="s3_zip_")
    local_zip_path = os.path.join(temp_dir, os.path.basename(zip_key))
    ts = now_ts()

    try:
        log.info("Downloading ZIP to local disk: %s", local_zip_path)
        s3.download_file(bucket, zip_key, local_zip_path)

        csv_files = get_csv_files(local_zip_path)
        small_files = [m for m in csv_files if m.file_size < split_size]
        large_files = [m for m in csv_files if m.file_size >= split_size]

        log.info("Small files: %s | Large files: %s", len(small_files), len(large_files))

        source_count_rows = []
        processed_count_rows = []

        if small_files:
            small_results = upload_small_files(bucket, target_folder, local_zip_path, zip_name, small_files, max_threads)
            for item in small_results:
                source_count_rows.append([
                    zip_name,
                    item["source_full_csv_file_name"],
                    item["row_count"],
                    has_header,
                    item["is_split"]
                ])
                processed_count_rows.append([
                    zip_name,
                    item["source_full_csv_file_name"],
                    item["target_full_csv_file_name"],
                    item["row_count"],
                    has_header,
                    item["is_split"],
                    item["split_files_count"]
                ])

        if large_files:
            log.info("Phase 2: processing large files one by one")
            for m in large_files:
                log.info("Splitting file: %s", m.filename)
                result = split_large_file(
                    bucket,
                    target_folder,
                    local_zip_path,
                    zip_name,
                    m.filename,
                    has_header,
                    split_size,
                    read_block,
                    max_threads
                )
                source_row = result["source_count_row"]
                source_count_rows.append([
                    zip_name,
                    source_row["source_full_csv_file_name"],
                    source_row["row_count"],
                    has_header,
                    source_row["is_split"]
                ])
                for row in result["processed_count_rows"]:
                    processed_count_rows.append([
                        zip_name,
                        row["source_full_csv_file_name"],
                        row["target_full_csv_file_name"],
                        row["row_count"],
                        has_header,
                        row["is_split"],
                        row["split_files_count"]
                    ])

        count_prefix = normalize_prefix(count_location)
        source_count_key = f"{count_prefix}/{zip_name}_source_count_{ts}.csv"
        processed_count_key = f"{count_prefix}/{zip_name}_processed_count_{ts}.csv"

        upload_count_file(
            bucket,
            source_count_key,
            [
                "zip_file_name",
                "source_full_csv_file_name",
                "row_count",
                "has_header",
                "is_split"
            ],
            source_count_rows
        )
        upload_count_file(
            bucket,
            processed_count_key,
            [
                "zip_file_name",
                "source_full_csv_file_name",
                "target_full_csv_file_name",
                "row_count",
                "has_header",
                "is_split",
                "split_files_count"
            ],
            processed_count_rows
        )

        log.info("Uploaded source count file: s3://%s/%s", bucket, source_count_key)
        log.info("Uploaded processed count file: s3://%s/%s", bucket, processed_count_key)

    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
            log.info("Deleted temp folder: %s", temp_dir)


def main():
    if len(sys.argv) != 3:
        print("Usage:")
        print("python3 unzip_s3_csv.py <s3_bucket> <parameter_file_location>")
        sys.exit(1)

    bucket = sys.argv[1]
    parameter_file_location = sys.argv[2]

    try:
        params = load_params_from_s3(bucket, parameter_file_location)

        source_folder = normalize_prefix(params["s3_source_files_location"])
        target_folder = normalize_prefix(params["s3_target_files_location"])
        count_location = normalize_prefix(params["count_location"])
        archive_folder = normalize_prefix(params["archive_files_location"])
        rejected_folder = normalize_prefix(params["rejected_files_location"])

        zip_input = str(params.get("zip_file_name", "ALL")).strip()
        has_header = bool(params.get("has_header", True))
        split_size = int(params.get("split_size_mb", 250)) * 1024 * 1024
        read_block = int(params.get("read_block_mb", 16)) * 1024 * 1024
        max_threads = int(params.get("max_threads", 6))

        zip_keys = get_zip_keys(bucket, source_folder, zip_input)

        if not zip_keys:
            log.info("No zip files found")
            return

        failed = False

        for zip_key in zip_keys:
            try:
                process_zip_file(
                    bucket=bucket,
                    target_folder=target_folder,
                    count_location=count_location,
                    zip_key=zip_key,
                    has_header=has_header,
                    split_size=split_size,
                    read_block=read_block,
                    max_threads=max_threads
                )
                archived_key = move_s3_object(bucket, zip_key, archive_folder)
                log.info("Archived ZIP successfully: s3://%s/%s", bucket, archived_key)
            except Exception:
                failed = True
                log.exception("Failed processing ZIP: %s", zip_key)
                try:
                    rejected_key = move_s3_object(bucket, zip_key, rejected_folder)
                    log.info("Moved failed ZIP to rejected: s3://%s/%s", bucket, rejected_key)
                except Exception:
                    log.exception("Failed moving ZIP to rejected folder: %s", zip_key)

        if failed:
            sys.exit(1)

        log.info("Done")

    except Exception:
        log.exception("Job failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
