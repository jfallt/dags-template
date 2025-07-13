from datetime import datetime
from os import path
from sys import path as sys_path

from pendulum import timezone
from dag_wrapper.environment import env_dag

# Add root directory to PATH in order to import common files for airflow
root_dir = path.dirname(path.dirname(path.abspath(__file__)))
sys_path.append(root_dir)
from common.aws import create_aws_conn
from constants.dist import internal

from config.constants import tim

# DAG Info & Docs

description = (
    "Keeps AWS credentials up to date for use in native Airflow Operators & Hooks"
)

@env_dag(
    start_date=datetime(2022, 1, 1, tzinfo=timezone("America/Toronto")),  # type: ignore
    schedule="0 * * * *",
    tags=["aws"],
    description=description,
    catchup=False,
    max_active_runs=1,
    prod_enabled=True,
)
def ops_dag(env):
    env = env._default  # get the Environment obj from DagParam

    for conn_id, args in {
        "example_role": {"role_arn": env.AIRFLOW_IAM_ROLE, "region_name": "us-east-1"},
        "example_chained_role": {
            "role_arn": env.AIRFLOW_IAM_ROLE,
            "chain_role_arns": [env.AIRFLOW_CHAIN_ROLE],
            "region_name": "us-east-1",
        },
    }.items():
        _create_aws_conn = create_aws_conn.override(task_id=conn_id)(
            args["role_arn"],
            "{{ var.value.HTTP_PROXY }}",
            "{{ var.value.HTTP_PROXY }}",
            conn_id,
            args.get("chain_role_arns", []),
            args.get("region_name", None),
        )

        _create_aws_conn


ops_dag()
