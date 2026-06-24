import sys
import io
import os
import json
import zipfile
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger(__name__)


def today_folder():
    return datetime.now().strftime("%Y%m%d")


def normalize_prefix(value):
    return str(value).strip().strip("/")


def load_params_from_s3(bucket, param_key):
    bootstrap_s3 = boto3.client("s3")
    response = bootstrap_s3.get_object(Bucket=bucket, Key=param_key)
    content = response["Body"].read().decode("utf-8")
    return json.loads(content)


def parse_bool(value, default=True):
    if value is None:
        return default
    if isinstance(value, bool):
        return value

    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False

    return default


def get_zip_keys(s3, bucket, source_folder, zip_input):
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


def is_valid_csv(member_name):
    name = member_name.replace("\\", "/").strip("/")
    base = os.path.basename(name)
    return (
        bool(name)
        and not name.endswith("/")
        and not name.startswith("__MACOSX/")
        and not base.startswith("._")
        and name.lower().endswith(".csv")
    )


def get_csv_files(zip_bytes):
    csv_files = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for member in zf.infolist():
            if is_valid_csv(member.filename):
                csv_files.append(member)
    return csv_files


def upload_bytes(s3, bucket, key, data, transfer_config):
    s3.upload_fileobj(
        Fileobj=io.BytesIO(data),
        Bucket=bucket,
        Key=key,
        ExtraArgs={"ContentType": "text/csv"},
        Config=transfer_config
    )


def move_s3_object(s3, bucket, source_key, target_base_prefix):
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


def build_small_file_key(target_folder, zip_name, member_name):
    clean_name = member_name.replace("\\", "/").strip("/")
    return f"{target_folder.rstrip('/')}/{today_folder()}/{zip_name}/all_small_files/{clean_name}"


def build_large_file_key(target_folder, zip_name, clean_name, file_base, part_number):
    return (
        f"{target_folder.rstrip('/')}/{today_folder()}/{zip_name}/"
        f"all_large_files/{file_base}/{file_base}_part{str(part_number).zfill(3)}.csv"
    )


def upload_small_file(
    s3,
    bucket,
    target_folder,
    zip_bytes,
    zip_name,
    member_name,
    transfer_config
):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        data = zf.read(member_name)

    clean_name = member_name.replace("\\", "/").strip("/")
    out_key = build_small_file_key(target_folder, zip_name, member_name)

    upload_bytes(s3, bucket, out_key, data, transfer_config)
    log.info("Uploaded small file: %s", out_key)

    return {
        "source_full_csv_file_name": clean_name,
        "target_full_csv_file_name": out_key,
        "is_split": False
    }


def upload_small_files(
    s3,
    bucket,
    target_folder,
    zip_bytes,
    zip_name,
    small_files,
    max_threads,
    transfer_config
):
    results = []

    log.info("Phase 1: uploading small files in parallel")

    with ThreadPoolExecutor(max_workers=max_threads) as pool:
        futures = [
            pool.submit(
                upload_small_file,
                s3,
                bucket,
                target_folder,
                zip_bytes,
                zip_name,
                m.filename,
                transfer_config
            )
            for m in small_files
        ]

        for future in as_completed(futures):
            results.append(future.result())

    return results


def split_large_file(
    s3,
    bucket,
    target_folder,
    zip_bytes,
    zip_name,
    member_name,
    has_header,
    split_size,
    read_block,
    max_threads,
    transfer_config
):
    clean_name = member_name.replace("\\", "/").strip("/")
    file_base = os.path.splitext(os.path.basename(clean_name))[0]

    header = b""
    leftover = b""
    current_part = bytearray()
    part_number = 1
    uploaded_rows = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
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

            upload_futures = []

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
                        out_key = build_large_file_key(
                            target_folder=target_folder,
                            zip_name=zip_name,
                            clean_name=clean_name,
                            file_base=file_base,
                            part_number=part_number
                        )
                        payload = header + bytes(current_part)

                        upload_futures.append(
                            pool.submit(
                                upload_bytes,
                                s3,
                                bucket,
                                out_key,
                                payload,
                                transfer_config
                            )
                        )

                        uploaded_rows.append({
                            "source_full_csv_file_name": clean_name,
                            "target_full_csv_file_name": out_key,
                            "is_split": True
                        })

                        log.info("Uploading split file: %s", out_key)
                        part_number += 1
                        current_part = bytearray()

                if leftover:
                    current_part.extend(leftover)

                if current_part:
                    out_key = build_large_file_key(
                        target_folder=target_folder,
                        zip_name=zip_name,
                        clean_name=clean_name,
                        file_base=file_base,
                        part_number=part_number
                    )
                    payload = header + bytes(current_part)

                    upload_futures.append(
                        pool.submit(
                            upload_bytes,
                            s3,
                            bucket,
                            out_key,
                            payload,
                            transfer_config
                        )
                    )

                    uploaded_rows.append({
                        "source_full_csv_file_name": clean_name,
                        "target_full_csv_file_name": out_key,
                        "is_split": True
                    })

                    log.info("Uploading split file: %s", out_key)

                for future in as_completed(upload_futures):
                    future.result()

    return uploaded_rows


def process_zip_file(
    s3,
    bucket,
    target_folder,
    zip_key,
    has_header,
    split_size,
    read_block,
    max_threads,
    transfer_config
):
    log.info("Processing ZIP: %s", zip_key)

    zip_bytes = s3.get_object(Bucket=bucket, Key=zip_key)["Body"].read()
    zip_name = os.path.splitext(os.path.basename(zip_key))[0]

    csv_files = get_csv_files(zip_bytes)
    small_files = [m for m in csv_files if m.file_size < split_size]
    large_files = [m for m in csv_files if m.file_size >= split_size]

    log.info("Small files: %s | Large files: %s", len(small_files), len(large_files))

    if small_files:
        upload_small_files(
            s3=s3,
            bucket=bucket,
            target_folder=target_folder,
            zip_bytes=zip_bytes,
            zip_name=zip_name,
            small_files=small_files,
            max_threads=max_threads,
            transfer_config=transfer_config
        )

    if large_files:
        log.info("Phase 2: processing large files one by one")
        for member in large_files:
            log.info("Splitting file: %s", member.filename)

            split_large_file(
                s3=s3,
                bucket=bucket,
                target_folder=target_folder,
                zip_bytes=zip_bytes,
                zip_name=zip_name,
                member_name=member.filename,
                has_header=has_header,
                split_size=split_size,
                read_block=read_block,
                max_threads=max_threads,
                transfer_config=transfer_config
            )

    del zip_bytes


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
        archive_folder = normalize_prefix(params["archive_files_location"])
        rejected_folder = normalize_prefix(params["rejected_files_location"])

        zip_input = str(params.get("zip_file_name", "ALL")).strip()
        has_header = parse_bool(params.get("has_header", True), default=True)

        split_size_mb = int(params.get("split_size_mb", 250))
        read_block_mb = int(params.get("read_block_mb", 16))
        max_threads = int(params.get("max_threads", min((os.cpu_count() or 4) * 2, 12)))
        max_pool_connections = int(params.get("max_pool_connections", max(50, max_threads * 4)))
        transfer_max_concurrency = int(params.get("transfer_max_concurrency", max_threads))

        split_size = split_size_mb * 1024 * 1024
        read_block = read_block_mb * 1024 * 1024

        s3_config = Config(
            max_pool_connections=max_pool_connections,
            retries={"max_attempts": 5, "mode": "adaptive"},
            tcp_keepalive=True
        )
        s3 = boto3.client("s3", config=s3_config)

        transfer_config = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,
            multipart_chunksize=8 * 1024 * 1024,
            max_concurrency=transfer_max_concurrency,
            use_threads=True
        )

        log.info(
            "S3 config | max_threads=%s | max_pool_connections=%s | transfer_max_concurrency=%s",
            max_threads,
            max_pool_connections,
            transfer_max_concurrency
        )

        zip_keys = get_zip_keys(s3, bucket, source_folder, zip_input)

        if not zip_keys:
            log.info("No zip files found. Exiting successfully.")
            sys.exit(0)

        failed = False

        for zip_key in zip_keys:
            try:
                process_zip_file(
                    s3=s3,
                    bucket=bucket,
                    target_folder=target_folder,
                    zip_key=zip_key,
                    has_header=has_header,
                    split_size=split_size,
                    read_block=read_block,
                    max_threads=max_threads,
                    transfer_config=transfer_config
                )

                archived_key = move_s3_object(s3, bucket, zip_key, archive_folder)
                log.info("Archived ZIP successfully: s3://%s/%s", bucket, archived_key)

            except Exception:
                failed = True
                log.exception("Failed processing ZIP: %s", zip_key)
                try:
                    rejected_key = move_s3_object(s3, bucket, zip_key, rejected_folder)
                    log.info("Moved failed ZIP to rejected: s3://%s/%s", bucket, rejected_key)
                except Exception:
                    log.exception("Failed moving ZIP to rejected folder: %s", zip_key)

        if failed:
            sys.exit(1)

        log.info("Done")
        sys.exit(0)

    except Exception:
        log.exception("Job failed")
        sys.exit(1)


if __name__ == "__main__":
    main()def parse_bool(value, default=True):
    if value is None:
        return default
    if isinstance(value, bool):
        return value

    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False

    return default


def get_zip_keys(s3, bucket, source_folder, zip_input):
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


def is_valid_csv(member_name):
    name = member_name.replace("\\", "/").strip("/")
    base = os.path.basename(name)
    return (
        bool(name)
        and not name.endswith("/")
        and not name.startswith("__MACOSX/")
        and not base.startswith("._")
        and name.lower().endswith(".csv")
    )


def get_csv_files(zip_bytes):
    csv_files = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for member in zf.infolist():
            if is_valid_csv(member.filename):
                csv_files.append(member)
    return csv_files


def upload_bytes(s3, bucket, key, data, transfer_config):
    s3.upload_fileobj(
        Fileobj=io.BytesIO(data),
        Bucket=bucket,
        Key=key,
        ExtraArgs={"ContentType": "text/csv"},
        Config=transfer_config
    )


def move_s3_object(s3, bucket, source_key, target_base_prefix):
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


def upload_small_file(
    s3,
    bucket,
    target_folder,
    zip_bytes,
    zip_name,
    member_name,
    transfer_config
):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        data = zf.read(member_name)

    clean_name = member_name.replace("\\", "/").strip("/")
    out_key = f"{target_folder.rstrip('/')}/{zip_name}/{clean_name}"

    upload_bytes(s3, bucket, out_key, data, transfer_config)
    log.info("Uploaded small file: %s", out_key)

    return {
        "source_full_csv_file_name": clean_name,
        "target_full_csv_file_name": out_key,
        "is_split": False
    }


def upload_small_files(
    s3,
    bucket,
    target_folder,
    zip_bytes,
    zip_name,
    small_files,
    max_threads,
    transfer_config
):
    results = []

    log.info("Phase 1: uploading small files in parallel")

    with ThreadPoolExecutor(max_workers=max_threads) as pool:
        futures = [
            pool.submit(
                upload_small_file,
                s3,
                bucket,
                target_folder,
                zip_bytes,
                zip_name,
                m.filename,
                transfer_config
            )
            for m in small_files
        ]

        for future in as_completed(futures):
            results.append(future.result())

    return results


def split_large_file(
    s3,
    bucket,
    target_folder,
    zip_bytes,
    zip_name,
    member_name,
    has_header,
    split_size,
    read_block,
    max_threads,
    transfer_config
):
    clean_name = member_name.replace("\\", "/").strip("/")
    file_base = os.path.splitext(os.path.basename(clean_name))[0]
    out_dir = f"{target_folder.rstrip('/')}/{zip_name}/{file_base}"

    header = b""
    leftover = b""
    current_part = bytearray()
    part_number = 1
    uploaded_rows = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
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

            upload_futures = []

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

                        upload_futures.append(
                            pool.submit(
                                upload_bytes,
                                s3,
                                bucket,
                                out_key,
                                payload,
                                transfer_config
                            )
                        )

                        uploaded_rows.append({
                            "source_full_csv_file_name": clean_name,
                            "target_full_csv_file_name": out_key,
                            "is_split": True
                        })

                        log.info("Uploading split file: %s", out_key)
                        part_number += 1
                        current_part = bytearray()

                if leftover:
                    current_part.extend(leftover)

                if current_part:
                    out_key = f"{out_dir}/{file_base}_part{str(part_number).zfill(3)}.csv"
                    payload = header + bytes(current_part)

                    upload_futures.append(
                        pool.submit(
                            upload_bytes,
                            s3,
                            bucket,
                            out_key,
                            payload,
                            transfer_config
                        )
                    )

                    uploaded_rows.append({
                        "source_full_csv_file_name": clean_name,
                        "target_full_csv_file_name": out_key,
                        "is_split": True
                    })

                    log.info("Uploading split file: %s", out_key)

                for future in as_completed(upload_futures):
                    future.result()

    return uploaded_rows


def process_zip_file(
    s3,
    bucket,
    target_folder,
    zip_key,
    has_header,
    split_size,
    read_block,
    max_threads,
    transfer_config
):
    log.info("Processing ZIP: %s", zip_key)

    zip_bytes = s3.get_object(Bucket=bucket, Key=zip_key)["Body"].read()
    zip_name = os.path.splitext(os.path.basename(zip_key))[0]

    csv_files = get_csv_files(zip_bytes)
    small_files = [m for m in csv_files if m.file_size < split_size]
    large_files = [m for m in csv_files if m.file_size >= split_size]

    log.info("Small files: %s | Large files: %s", len(small_files), len(large_files))

    if small_files:
        upload_small_files(
            s3=s3,
            bucket=bucket,
            target_folder=target_folder,
            zip_bytes=zip_bytes,
            zip_name=zip_name,
            small_files=small_files,
            max_threads=max_threads,
            transfer_config=transfer_config
        )

    if large_files:
        log.info("Phase 2: processing large files one by one")
        for member in large_files:
            log.info("Splitting file: %s", member.filename)

            split_large_file(
                s3=s3,
                bucket=bucket,
                target_folder=target_folder,
                zip_bytes=zip_bytes,
                zip_name=zip_name,
                member_name=member.filename,
                has_header=has_header,
                split_size=split_size,
                read_block=read_block,
                max_threads=max_threads,
                transfer_config=transfer_config
            )

    del zip_bytes


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
        archive_folder = normalize_prefix(params["archive_files_location"])
        rejected_folder = normalize_prefix(params["rejected_files_location"])

        zip_input = str(params.get("zip_file_name", "ALL")).strip()
        has_header = parse_bool(params.get("has_header", True), default=True)

        split_size_mb = int(params.get("split_size_mb", 250))
        read_block_mb = int(params.get("read_block_mb", 16))
        max_threads = int(params.get("max_threads", min((os.cpu_count() or 4) * 2, 12)))
        max_pool_connections = int(params.get("max_pool_connections", max(50, max_threads * 4)))
        transfer_max_concurrency = int(params.get("transfer_max_concurrency", max_threads))

        split_size = split_size_mb * 1024 * 1024
        read_block = read_block_mb * 1024 * 1024

        s3_config = Config(
            max_pool_connections=max_pool_connections,
            retries={"max_attempts": 5, "mode": "adaptive"},
            tcp_keepalive=True
        )
        s3 = boto3.client("s3", config=s3_config)

        transfer_config = TransferConfig(
            multipart_threshold=8 * 1024 * 1024,
            multipart_chunksize=8 * 1024 * 1024,
            max_concurrency=transfer_max_concurrency,
            use_threads=True
        )

        log.info(
            "S3 config | max_threads=%s | max_pool_connections=%s | transfer_max_concurrency=%s",
            max_threads,
            max_pool_connections,
            transfer_max_concurrency
        )

        zip_keys = get_zip_keys(s3, bucket, source_folder, zip_input)

        if not zip_keys:
            log.info("No zip files found. Exiting successfully.")
            sys.exit(0)

        failed = False

        for zip_key in zip_keys:
            try:
                process_zip_file(
                    s3=s3,
                    bucket=bucket,
                    target_folder=target_folder,
                    zip_key=zip_key,
                    has_header=has_header,
                    split_size=split_size,
                    read_block=read_block,
                    max_threads=max_threads,
                    transfer_config=transfer_config
                )

                archived_key = move_s3_object(s3, bucket, zip_key, archive_folder)
                log.info("Archived ZIP successfully: s3://%s/%s", bucket, archived_key)

            except Exception:
                failed = True
                log.exception("Failed processing ZIP: %s", zip_key)
                try:
                    rejected_key = move_s3_object(s3, bucket, zip_key, rejected_folder)
                    log.info("Moved failed ZIP to rejected: s3://%s/%s", bucket, rejected_key)
                except Exception:
                    log.exception("Failed moving ZIP to rejected folder: %s", zip_key)

        if failed:
            sys.exit(1)

        log.info("Done")
        sys.exit(0)

    except Exception:
        log.exception("Job failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
