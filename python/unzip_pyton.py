import json
import logging
from datetime import datetime
from io import BytesIO

import boto3
from botocore.exceptions import ClientError
import zipfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)

def get_s3_client(max_pool_connections: int = 50):
    session = boto3.Session()
    config = boto3.session.Config(max_pool_connections=max_pool_connections)
    return session.client("s3", config=config)

def load_json_config_from_s3(s3_client, bucket: str, config_key: str) -> dict:
    logging.info(f"Reading config from s3://{bucket}/{config_key}")
    obj = s3_client.get_object(Bucket=bucket, Key=config_key)
    content = obj["Body"].read().decode("utf-8")
    cfg = json.loads(content)
    logging.info(f"Loaded config: {cfg}")
    return cfg

def list_zip_keys(s3_client, bucket: str, prefix: str) -> list:
    keys = []
    continuation_token = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = s3_client.list_objects_v2(**kwargs)
        contents = resp.get("Contents", [])
        for obj in contents:
            key = obj["Key"]
            if key.lower().endswith(".zip"):
                keys.append(key)

        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break

    logging.info(f"Found ZIP keys under {prefix}: {keys}")
    return keys

def delete_files_in_unzip_subfolder(
    s3_client,
    bucket: str,
    unzip_root: str,
    zip_key: str,
    skip_files: list,
):
    """
    Delete any objects in s3_unzip_folder/<zip_base>/ whose basename
    is in skip_files.
    """
    if not skip_files:
        return

    zip_base = zip_key.split("/")[-1].rsplit(".", 1)[0]
    prefix = f"{unzip_root}{zip_base}/"
    logging.info(
        f"Deleting skipped files under s3://{bucket}/{prefix} with names: {skip_files}"
    )

    continuation_token = None
    to_delete = []

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = s3_client.list_objects_v2(**kwargs)
        contents = resp.get("Contents", [])
        if not contents:
            break

        for obj in contents:
            key = obj["Key"]
            basename = key.split("/")[-1]
            if basename in skip_files:
                to_delete.append({"Key": key})

        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break

    if to_delete:
        logging.info(f"Deleting {len(to_delete)} objects from unzip subfolder")
        s3_client.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
    else:
        logging.info("No matching files to delete in unzip subfolder")

def unzip_s3_zip_to_s3_per_zip_folder(
    s3_client,
    bucket: str,
    zip_key: str,
    unzip_root: str,
    skip_files: list,
):
    """
    Unzip one S3 ZIP to S3 under:
      unzip_root/<zip_base>/<member_name>

    Skip any members whose basename is in skip_files.
    """
    zip_base = zip_key.split("/")[-1].rsplit(".", 1)[0]
    unzip_prefix_for_zip = f"{unzip_root}{zip_base}/"
    logging.info(
        f"Unzipping s3://{bucket}/{zip_key} -> prefix {unzip_prefix_for_zip}"
    )

    zip_obj = s3_client.get_object(Bucket=bucket, Key=zip_key)
    buffer = BytesIO(zip_obj["Body"].read())

    with zipfile.ZipFile(buffer) as zf:
        for member_name in zf.namelist():
            # Skip directories inside ZIP
            if member_name.endswith("/"):
                continue

            basename = member_name.split("/")[-1]
            if basename in skip_files:
                logging.info(
                    f"Skipping member {member_name} (basename '{basename}' in skip_files)"
                )
                continue

            target_key = f"{unzip_prefix_for_zip}{member_name}"
            logging.info(
                f"Uploading member {member_name} -> s3://{bucket}/{target_key}"
            )

            with zf.open(member_name) as member_fp:
                s3_client.upload_fileobj(
                    Fileobj=member_fp,
                    Bucket=bucket,
                    Key=target_key,
                )

    logging.info(f"Finished unzipping {zip_key}")

def archive_zip(
    s3_client,
    bucket: str,
    zip_key: str,
    archive_prefix: str,
    archive_date: str,
    delete_after: bool,
):
    """
    Copy ZIP to archive folder. Optionally delete original.
    """
    zip_name = zip_key.split("/")[-1]
    archive_key = f"{archive_prefix}{archive_date}/ZIP_FILE/{zip_name}"
    logging.info(f"Archiving ZIP {zip_key} -> s3://{bucket}/{archive_key}")

    s3_client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": zip_key},
        Key=archive_key,
    )

    if delete_after:
        logging.info(f"Deleting original ZIP: s3://{bucket}/{zip_key}")
        s3_client.delete_object(Bucket=bucket, Key=zip_key)
    else:
        logging.info("delete_zip_after_archive is False, keeping original ZIP")

def main():
    import sys

    if len(sys.argv) != 3:
        print(
            "Usage: python s3_unzip_archive.py <s3_bucket> <config_key>\n"
            "Example: python s3_unzip_archive.py my-bucket config/unzip_job.json"
        )
        sys.exit(1)

    bucket = sys.argv[1]
    config_key = sys.argv[2]

    s3_client = get_s3_client(max_pool_connections=50)

    try:
        cfg = load_json_config_from_s3(s3_client, bucket, config_key)
    except ClientError as e:
        logging.error(f"Failed to load config: {e}")
        sys.exit(1)

    zip_folder = cfg.get("s3_zip_folder", "")
    archive_folder = cfg.get("s3_archive_folder", "")
    zip_file_name = cfg.get("zip_file_name", "ALL")
    delete_zip_after_archive = bool(cfg.get("delete_zip_after_archive", True))
    skip_files = cfg.get("skip_files", []) or []
    # Each rule: {"prefix": "abc_", "unzip_folder": "Prj_unzip/SOURCE_FILES/"}
    # Zips whose name matches no prefix are archived without unzipping.
    routing_rules = cfg.get("routing_rules", []) or []

    # Normalize prefixes
    def ensure_suffix_slash(p: str) -> str:
        if p and not p.endswith("/"):
            return p + "/"
        return p

    zip_folder = ensure_suffix_slash(zip_folder)
    archive_folder = ensure_suffix_slash(archive_folder)
    for rule in routing_rules:
        rule["unzip_folder"] = ensure_suffix_slash(rule.get("unzip_folder", ""))

    # Determine ZIPs to process
    zip_keys = []
    if zip_file_name == "ALL":
        zip_keys = list_zip_keys(s3_client, bucket, zip_folder)
    else:
        zip_key = f"{zip_folder}{zip_file_name}"
        try:
            s3_client.head_object(Bucket=bucket, Key=zip_key)
            zip_keys = [zip_key]
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logging.warning(f"ZIP not found: s3://{bucket}/{zip_key}, skipping.")
            else:
                logging.error(f"Error checking ZIP {zip_key}: {e}")
                sys.exit(1)

    if not zip_keys:
        logging.info("No ZIP files to process, exiting cleanly.")
        return

    archive_date = datetime.now().strftime("%Y%m%d")

    # Process each ZIP
    for zip_key in zip_keys:
        zip_basename = zip_key.split("/")[-1]

        # Resolve destination unzip folder based on zip filename prefix
        matched_unzip_root = None
        for rule in routing_rules:
            prefix = rule.get("prefix", "")
            if prefix and zip_basename.lower().startswith(prefix.lower()):
                matched_unzip_root = rule["unzip_folder"]
                logging.info(
                    f"{zip_basename} matched prefix '{prefix}' -> unzip root {matched_unzip_root}"
                )
                break

        if matched_unzip_root is None:
            # No routing rule matched: skip unzip, archive the ZIP as-is
            logging.info(
                f"{zip_basename} matched no routing rule, archiving without unzipping"
            )
            archive_zip(
                s3_client=s3_client,
                bucket=bucket,
                zip_key=zip_key,
                archive_prefix=archive_folder,
                archive_date=archive_date,
                delete_after=delete_zip_after_archive,
            )
            continue

        unzip_s3_zip_to_s3_per_zip_folder(
            s3_client=s3_client,
            bucket=bucket,
            zip_key=zip_key,
            unzip_root=matched_unzip_root,
            skip_files=skip_files,
        )

        # Delete any existing files matching skip_files in this ZIP's unzip subfolder
        delete_files_in_unzip_subfolder(
            s3_client=s3_client,
            bucket=bucket,
            unzip_root=matched_unzip_root,
            zip_key=zip_key,
            skip_files=skip_files,
        )

        archive_zip(
            s3_client=s3_client,
            bucket=bucket,
            zip_key=zip_key,
            archive_prefix=archive_folder,
            archive_date=archive_date,
            delete_after=delete_zip_after_archive,
        )

if __name__ == "__main__":
    main()
