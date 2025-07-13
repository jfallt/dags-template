import json
import logging
import re
from os import listdir, path
from sys import path as sys_path

from airflow.datasets import Dataset, DatasetAlias
from airflow.datasets.metadata import Metadata
from airflow.decorators import task, task_group


@task(trigger_rule="none_failed")
def return_dataset(
    dataset_uri,
    s3_path,
    metadata={},
    alias={},
    outlet_events=None,
    **kwargs,
):
    """
    Creates and returns a Dataset object with associated metadata.

    This task creates a Dataset object for the given URI, attaches metadata including the S3 path,
    and associates the dataset with any provided aliases that have production enabled.

    Args:
        dataset_uri (str): The URI identifier for the dataset.
        s3_path (str): The S3 path where the dataset is stored.
        outlet_events (dict): Dictionary to track dataset outlet events.
        metadata (dict or str, optional): Additional metadata to attach to the dataset.
            If provided as a string, will attempt to parse it as a Python literal.
            Defaults to an empty dict.
        aliases (dict, optional): Dictionary of dataset aliases where the dataset should be added.
            Only aliases with {'prod_enabled': True} will be used. Defaults to an empty dict.
        **kwargs: Additional keyword arguments.

    Yields:
        Metadata: Metadata object containing information about the dataset, including the S3 path.

    Returns:
        Dataset: The created dataset object.
    """
    from ast import literal_eval

    dataset = Dataset(dataset_uri)
    if metadata is None:
        metadata = {}
    elif isinstance(metadata, str):
        try:
            metadata = literal_eval(metadata)
            if not isinstance(metadata, dict):
                metadata = {}
        except (SyntaxError, ValueError):
            metadata = {}
    elif not isinstance(metadata, dict):
        metadata = {}
    dataset_metadata = Metadata(dataset, extra={"s3_path": s3_path, **metadata})
    yield dataset_metadata

@task
def fetch_extra_from_latest_dataset_event(
    target_dataset, extra: str | list = "file_path", **context
):
    """
    Gets only the most recent Dataset to trigger a DAG.
    Since each Dataset event has a incremented id, this uses the
    highest id in triggering_dataset_events related to the target_dataset.
    """
    triggering_dataset_events = context["triggering_dataset_events"]
    for dataset, dataset_list in triggering_dataset_events.items():
        if dataset == target_dataset:
            most_recent_event = max(dataset_list, key=lambda event: event.id)

            if isinstance(extra, str):
                return most_recent_event.extra.get(extra)
            elif isinstance(extra, list):
                result = {}
                for key in extra:
                    result[key] = most_recent_event.extra.get(key)
                    context["ti"].xcom_push(
                        key=key, value=most_recent_event.extra.get(key)
                    )
                return result
            return None

