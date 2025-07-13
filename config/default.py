from datetime import timedelta

AIRFLOW__IAM_ROLE = "{{ var.value.iam_opsrole}}"
AIRFLOW__TEAM = "team"
AIRFLOW__CONTAINER_ENV_VARS = {
    "s3_proxy": "{{ var.value.S3_PROXY }}",
    "S3_PROXY": "{{ var.value.S3_PROXY }}",
    "http_proxy": "{{ var.value.HTTP_PROXY}}",
    "https_proxy": "{{ var.value.HTTP_PROXY}}",
    "HTTP_PROXY": "{{ var.value.HTTP_PROXY}}",
    "HTTPS_PROXY": "{{ var.value.HTTP_PROXY}}",
    "no_proxy": "{{ var.value.NO_PROXY}}",
    "NO_PROXY": "{{ var.value.NO_PROXY}}",
}

DAG__DEFAULT_ARGS = {
    "owner": "ops",
    "depends_on_past": False,
    "email": [],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}
DAG__ACCESS_CONTROL = {
    "team_op": {"can_read", "can_edit", "can_delete"},
    "team_viewer": {"can_read"},
}
DAG__DEFAULT_PARAMS = {"is_paused_upon_creation": False}
