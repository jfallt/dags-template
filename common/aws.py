import logging
import json
import boto3  # type: ignore
import requests
from scripts.aws import parse_sqs_message_for_key
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow import settings
from airflow.decorators import task
from airflow.models import Connection
import dns.resolver
from urllib.parse import unquote_plus
from airflow.exceptions import AirflowException


def fetch_iam_credentials(iam_role, session_name):
    logging.info(f"Assuming role {iam_role}...")
    stsclient = boto3.client("sts")
    response = stsclient.assume_role(RoleArn=iam_role, RoleSessionName=session_name)
    logging.info("Assumed role!")
    response["Credentials"]["Region"] = requests.get(
        "http://169.254.169.254/latest/meta-data/placement/availability-zone"
    ).text[:-1]
    logging.info(response)
    return response["Credentials"]


@task
def create_aws_conn(role_arn, http_proxy, https_proxy, conn_id="ops_sqs_queue"):
    """
    Creates a dynamic connection for AWS

    Followed the guide below to set up:
        https://github.com/apache/airflow/blob/main/docs/apache-airflow-providers-amazon/connections/aws.rst
    """
    instance_creds = fetch_iam_credentials(role_arn, conn_id)
    logging.info("Creating AWS connection object...")
    conn = Connection(
        conn_id=conn_id,
        conn_type="aws",
        description="Temporary connection to read OPS SQS queue",
        login=instance_creds["AccessKeyId"],
        extra={
            "aws_session_token": instance_creds["SessionToken"],
            "region_name": instance_creds["Region"],
            "config_kwargs": {
                "proxies": {
                    "http": http_proxy,
                    "https": https_proxy,
                }
            },
        },
    )
    conn.set_password(instance_creds["SecretAccessKey"])

    logging.info("Checking if we have an existing connection with the same id...")
    session = settings.Session()
    conn_id = (
        session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    )
    if conn_id:
        logging.info(f"A connection with `conn_id`={conn_id} already exists")
        session.delete(conn_id)
        session.commit()
        logging.info("Deleted existing conn_id")
    else:
        logging.info(f"No existing connection with `conn_id`={conn_id}")
    logging.info(f"Adding new connection with id={conn_id}")
    session.add(conn)
    session.commit()
    logging.info("Complete!")
    return instance_creds["Expiration"]


@task
def parse_sqs_messages_as_dict(messages: list, prefix: str | None = None, **kwargs):
    """
    Airflow task parsing each message in the messages xcom.
    Refer to parse_sqs_message_for_key for parsing logic.

    Args:
        messages (list): list of SQS messages to parse
        prefix (str): If there is a base prefix to remove

    Returns:
        list of dict: [{path_to_file: prefix/path, filename: file.txt}, {path_to_file: prefix1/path1, filename: file1.txt}, ...]
    """
    path_and_file_list = []
    for entry in messages:
        path_to_file, filename = parse_sqs_message_for_key(entry, prefix)
        # Bucket notifications treat s3 paths as urls
        path_and_file_list.append(
            {"path_to_file": path_to_file, "filename": unquote_plus(filename)}
        )
    return path_and_file_list


@task
def read_csv_cell_from_s3(
    column: str,
    aws_conn_id: str,
    bucket_name: str | None = None,
    file_key: str | None = None,
    s3_path: str | None = None,
):
    """
    Read the first cell from a CSV file stored in S3.

    Use case: when a column contains a single value for records (i.e. EFFECTIVE_DATE)
    """
    import csv
    import io

    if s3_path:
        # Split the s3_path into bucket_name and file_key
        if not s3_path.startswith("s3://"):
            raise AirflowException("s3_path must start with 's3://'")
        s3_path_parts = s3_path[5:].split("/", 1)
        if len(s3_path_parts) < 2:
            raise AirflowException("s3_path must include both bucket name and file key")
        bucket_name, file_key = s3_path_parts

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
    file_content = obj.get()["Body"].read().decode("utf-8")

    # Use csv.DictReader to directly parse the CSV content
    reader = csv.DictReader(io.StringIO(file_content))

    # Extract the COLUMN from the first row
    first_row = next(reader, None)
    if not first_row or column not in first_row:
        raise AirflowException(f"{column} not found in the file or file is empty.")

    return first_row[column]
