# download
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
from botocore.exceptions import ClientError
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


def ensure_s3_object_exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            log.error("Source file not found: s3://%s/%s", bucket, key)
            sys.exit(1)
        raise


def get_zip_keys(s3, bucket, source_folder, zip_files_config):
    zip_keys = []

    all_zip_files = parse_bool(zip_files_config.get("all_zip_files"), default=False)
    zip_file_name = str(zip_files_config.get("zip_file_name", "")).strip()

    if all_zip_files:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=source_folder.rstrip("/") + "/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.lower().endswith(".zip"):
                    zip_keys.append(key)
    else:
        if not zip_file_name:
            log.error("zip_file_name is required when all_zip_files is false")
            sys.exit(1)

        zip_key = zip_file_name.strip("/")
        if not zip_key.startswith(source_folder.rstrip("/") + "/"):
            zip_key = source_folder.rstrip("/") + "/" + zip_key

        zip_keys.append(zip_key)

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


def normalize_member_name(member_name):
    return member_name.replace("\\", "/").strip("/")


def get_csv_files(local_zip_path, file_config):
    csv_files = []

    all_file = parse_bool(file_config.get("all_file"), default=True)
    file_names = file_config.get("file_names")

    selected_names = set()
    if not all_file:
        if not file_names:
            log.error("file.file_names is required when file.all_file is false")
            sys.exit(1)
        if not isinstance(file_names, list):
            log.error("file.file_names must be a list")
            sys.exit(1)

        selected_names = {
            normalize_member_name(name)
            for name in file_names
            if str(name).strip()
        }

    with zipfile.ZipFile(local_zip_path) as zf:
        for member in zf.infolist():
            if not is_valid_csv(member.filename):
                continue

            clean_name = normalize_member_name(member.filename)
            base_name = os.path.basename(clean_name)

            if all_file or clean_name in selected_names or base_name in selected_names:
                csv_files.append(member)

    if not all_file and not csv_files:
        log.error("No matching CSV files found in ZIP for requested file.file_names")
        sys.exit(1)

    return csv_files


def upload_bytes(s3, bucket, key, data, transfer_config):
    s3.upload_fileobj(
        Fileobj=io.BytesIO(data),
        Bucket=bucket,
        Key=key,
        ExtraArgs={"ContentType": "text/csv"},
        Config=transfer_config
    )


def move_s3_object(s3, bucket, source_key, target_base_prefix, transfer_config):
    base_prefix = normalize_prefix(target_base_prefix)
    dated_prefix = f"{base_prefix}/{today_folder()}"
    target_key = f"{dated_prefix}/{os.path.basename(source_key)}"

    log.info("Moving file from s3://%s/%s to s3://%s/%s", bucket, source_key, bucket, target_key)

    copy_source = {
        "Bucket": bucket,
        "Key": source_key
    }

    s3.copy(
        CopySource=copy_source,
        Bucket=bucket,
        Key=target_key,
        Config=transfer_config
    )
    s3.delete_object(Bucket=bucket, Key=source_key)

    return target_key


def build_small_file_key(target_folder, zip_name, member_name):
    clean_name = normalize_member_name(member_name)
    return f"{target_folder.rstrip('/')}/{today_folder()}/{zip_name}/{clean_name}"


def build_large_file_key(target_folder, zip_name, file_base, part_number):
    return (
        f"{target_folder.rstrip('/')}/{today_folder()}/{zip_name}/"
        f"{file_base}/{file_base}_part{str(part_number).zfill(3)}.csv"
    )


def upload_small_file(
    s3,
    bucket,
    target_folder,
    local_zip_path,
    zip_name,
    member_name,
    transfer_config
):
    with zipfile.ZipFile(local_zip_path) as zf:
        data = zf.read(member_name)

    clean_name = normalize_member_name(member_name)
    out_key = build_small_file_key(target_folder, zip_name, member_name)

    upload_bytes(s3, bucket, out_key, data, transfer_config)
    log.info("Uploaded full file: %s", out_key)

    return {
        "source_full_csv_file_name": clean_name,
        "target_full_csv_file_name": out_key,
        "is_split": False
    }


def upload_small_files(
    s3,
    bucket,
    target_folder,
    local_zip_path,
    zip_name,
    files_to_upload,
    max_threads,
    transfer_config
):
    results = []

    log.info("Uploading full files in parallel")

    with ThreadPoolExecutor(max_workers=max_threads) as pool:
        futures = [
            pool.submit(
                upload_small_file,
                s3,
                bucket,
                target_folder,
                local_zip_path,
                zip_name,
                m.filename,
                transfer_config
            )
            for m in files_to_upload
        ]

        for future in as_completed(futures):
            results.append(future.result())

    return results


def split_large_file(
    s3,
    bucket,
    target_folder,
    local_zip_path,
    zip_name,
    member_name,
    has_header,
    split_size,
    read_block,
    max_threads,
    transfer_config
):
    clean_name = normalize_member_name(member_name)
    file_base = os.path.splitext(os.path.basename(clean_name))[0]

    header = b""
    leftover = b""
    current_part = bytearray()
    part_number = 1
    uploaded_rows = []

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
    file_config,
    split_enabled,
    has_header,
    split_size,
    read_block,
    max_threads,
    transfer_config
):
    log.info("Processing ZIP: %s", zip_key)

    zip_name = os.path.splitext(os.path.basename(zip_key))[0]
    local_zip_path = os.path.join(os.getcwd(), os.path.basename(zip_key))

    try:
        ensure_s3_object_exists(s3, bucket, zip_key)

        log.info("Downloading ZIP to current working directory: %s", local_zip_path)
        s3.download_file(bucket, zip_key, local_zip_path)

        csv_files = get_csv_files(local_zip_path, file_config)

        if split_enabled:
            small_files = [m for m in csv_files if m.file_size < split_size]
            large_files = [m for m in csv_files if m.file_size >= split_size]

            log.info("Split enabled | Small files: %s | Large files: %s", len(small_files), len(large_files))

            if small_files:
                upload_small_files(
                    s3=s3,
                    bucket=bucket,
                    target_folder=target_folder,
                    local_zip_path=local_zip_path,
                    zip_name=zip_name,
                    files_to_upload=small_files,
                    max_threads=max_threads,
                    transfer_config=transfer_config
                )

            if large_files:
                log.info("Processing large files with splitting")
                for member in large_files:
                    log.info("Splitting file: %s", member.filename)
                    split_large_file(
                        s3=s3,
                        bucket=bucket,
                        target_folder=target_folder,
                        local_zip_path=local_zip_path,
                        zip_name=zip_name,
                        member_name=member.filename,
                        has_header=has_header,
                        split_size=split_size,
                        read_block=read_block,
                        max_threads=max_threads,
                        transfer_config=transfer_config
                    )
        else:
            log.info("Split disabled | Uploading selected CSV files as full files")
            upload_small_files(
                s3=s3,
                bucket=bucket,
                target_folder=target_folder,
                local_zip_path=local_zip_path,
                zip_name=zip_name,
                files_to_upload=csv_files,
                max_threads=max_threads,
                transfer_config=transfer_config
            )

    finally:
        if os.path.exists(local_zip_path):
            os.remove(local_zip_path)
            log.info("Deleted local ZIP file: %s", local_zip_path)


def main():
    if len(sys.argv) != 3:
        print("Usage:")
        print("python3 unzip_rom.py <s3_bucket> <parameter_file_location>")
        sys.exit(1)

    bucket = sys.argv[1]
    parameter_file_location = sys.argv[2]

    try:
        params = load_params_from_s3(bucket, parameter_file_location)

        source_folder = normalize_prefix(params["s3_source_files_location"])
        target_folder = normalize_prefix(params["s3_target_files_location"])
        rejected_folder = normalize_prefix(params["rejected_files_location"])

        archive_config = params.get("archive", {})
        archive_enabled = parse_bool(archive_config.get("archive"), default=True)
        archive_folder = normalize_prefix(archive_config.get("archive_files_location", "")) if archive_enabled else ""

        zip_files_config = params["zip_files"]
        file_config = zip_files_config.get("file", {})
        split_enabled = parse_bool(zip_files_config.get("split"), default=True)

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
            retries={"max_attempts": 5, "mode": "adaptive"}
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

        zip_keys = get_zip_keys(s3, bucket, source_folder, zip_files_config)

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
                    file_config=file_config,
                    split_enabled=split_enabled,
                    has_header=has_header,
                    split_size=split_size,
                    read_block=read_block,
                    max_threads=max_threads,
                    transfer_config=transfer_config
                )

                if archive_enabled:
                    if not archive_folder:
                        log.error("archive.archive_files_location is required when archive.archive is true")
                        sys.exit(1)

                    archived_key = move_s3_object(
                        s3=s3,
                        bucket=bucket,
                        source_key=zip_key,
                        target_base_prefix=archive_folder,
                        transfer_config=transfer_config
                    )
                    log.info("Archived ZIP successfully: s3://%s/%s", bucket, archived_key)
                else:
                    log.info("Archive disabled | Source ZIP left in place: s3://%s/%s", bucket, zip_key)

            except SystemExit:
                raise
            except Exception:
                failed = True
                log.exception("Failed processing ZIP: %s", zip_key)

                try:
                    ensure_s3_object_exists(s3, bucket, zip_key)

                    rejected_key = move_s3_object(
                        s3=s3,
                        bucket=bucket,
                        source_key=zip_key,
                        target_base_prefix=rejected_folder,
                        transfer_config=transfer_config
                    )
                    log.info("Moved failed ZIP to rejected: s3://%s/%s", bucket, rejected_key)

                except SystemExit:
                    raise
                except Exception:
                    log.exception("Failed moving ZIP to rejected folder: %s", zip_key)

        if failed:
            sys.exit(1)

        log.info("Done")
        sys.exit(0)

    except SystemExit:
        raise
    except Exception:
        log.exception("Job failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
