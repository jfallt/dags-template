import inspect
import logging
from functools import wraps
from importlib import import_module
from os import path, getcwd, environ
from pathlib import Path
from sys import path as sys_path
from typing import List
import re

import yaml
from airflow.decorators import dag
from airflow.models import Variable

# Add root directory to PATH in order to import common files for airflow
plugins_directory = path.dirname(path.abspath(__file__))
if plugins_directory not in sys_path:
    sys_path.append(plugins_directory)


class Environment:
    """This represents a single environment. It can be used to directly access the
    variables set for the current environment a DAG is running in. All environment
    specific variables are set in config/ by default

    For example, the following will read data from config/uat.py

    env = Environment('uat')
    print(env.AIRFLOW__SECRETS)
    >> {'secret1': ... }
    """

    REQUIRED_CONFIG_VARS = [
        "AIRFLOW__TEAM",
        "AIRFLOW__SECRETS_PREFIX",  # e.g. MAM_CORE
        "AIRFLOW__IAM_ROLE",
        "AIRFLOW__DEFAULT_IMAGE",
        "AIRFLOW__CONTAINER_ENV_VARS",
        "DAG__DEFAULT_PARAMS",
        "DAG__DEFAULT_ARGS",
        "DAG__ACCESS_CONTROL",
    ]

    TIER_DEFAULT = "default"
    TIER_PROD = "prod"
    TIER_NON = "non"
    VALID_LOWER_ENVS = ["dev", "sit", "uat"]

    module = None
    tier_str = None
    tier_obj = None

    def __init__(self, env: str, env_path="config", dags_home: str = getcwd()):
        self.env = env
        self.env_path = env_path
        self.dags_home = dags_home
        self.context = {}

        self.tier_str = self.get_tier_from_env()
        if self.tier_str and not self.load_context():
            # fetch the parent tier as an Env object
            self.tier_obj = Environment(self.tier_str, env_path, dags_home)

        self._validate_required_env_vars()

    def __getattr__(self, attr):
        """Access all environment variables as attributes on this object.
        If nothing is found, return None."""
        if self.load_context():
            return self.context.get(attr)

        if not self.module:
            try:
                # Trying to find module in the parent package
                self.module = import_module(self.env_path, package=self.env)
            except ImportError:
                logging.error("Relative import of environment failed")

            try:
                # Trying to find module on sys.path
                self.module = import_module(f"{self.env_path}.{self.env}")
            except ModuleNotFoundError:
                logging.error("Absolute import of environment failed")
            if self.module is None:
                raise Exception(f"Config ({self.env}.py) not found in {self.dags_home}{self.env_path} or section missing in context.yaml")
        try:
            return getattr(self.module, attr)
        except AttributeError:
            if self.tier_obj:
                return getattr(self.tier_obj, attr)

    def __deepcopy__(self, memo=None):
        return Environment(self.env, self.env_path, self.dags_home)

    def __repr__(self):
        return f"Environment({self.env})"

    def __eq__(self, other):
        return other.__hash__() == self.__hash__()

    def __hash__(self):
        return hash(self.env)

    def get_tier_from_env(self):
        """The tiers propagate up recursively. If an environment attribute is not found on the env object,
        it will search the parent tier for the information in the following order:

        dev|sit|uat -> non -> default
        prod -> default
        """
        if self.env == self.TIER_DEFAULT:
            return None
        elif self.env in [Environment.TIER_NON, Environment.TIER_PROD]:
            return self.TIER_DEFAULT
        return self.TIER_NON

    def load_context(self):
        """If a context.yaml file exists at the env_path, load the current environments context."""
        if self.context:
            return True

        config_path: Path = Path(self.dags_home, self.env_path, "context.yaml")

        if config_path.exists():
            self.context = yaml.safe_load(config_path.read_text())[self.env]
            return True
        else:
            return False

    def _validate_required_env_vars(self):
        for v in self.REQUIRED_CONFIG_VARS:
            if not getattr(self, v):
                raise Exception(f"Missing required env '{v}", self.REQUIRED_CONFIG_VARS)


class env_dag:
    """This class is a Decorator that extends the @dag decorator.

    It will:

    1 - Automatically determine which environment the DAG is running in provide an env object that contains
    all environment specific variables.

    Environment specific variables can be stored in python constant files named the same as the environment:

    config/dev.py - vars specific to the dev non production environment, if env exists
    config/sit.py - vars specific to the sit non production environment, if env exists
    config/uat.py - vars specific to the uat non production environment, if env exists
    config/non.py - vars specific to all non production environments
    config/prod.py - vars specific to production
    config/default.py - will use constants from this file if cannot be found anywhere else, non env specific constants.

    Alternatively, you can store all env-specific variables in a single context.yaml file.

    change env_path to specify the path to the environment variable configuration from 'config/' to another location.

    It also adds the following functionality to the @dag() decorator:

    2 - 'prod_enabled' (bool), (optional, defaults to False): If True, it will add the DAG to the dagbag in prod.
    3 - 'lower_envs' (list), (optional, defaults to 'non'): If set, it will generate a DAG for every lower environment specified,
    giving each a suffix of the environment (i.e. test_dag -> test_dag_dev). See Environment.VALID_LOWER_ENVS.

    To run as a python script for different deployment/environments locally you can set the following ENV VARS:
        - AIRFLOW_SERVER_VERSION = 1 or 2
        - CORE_AIRFLOW_ENVIRONMENT = non or prod (set if AIRFLOW_SERVER_VERSION=1)
        - CORE_AIRFLOW_CLUSTER_CONTEXT = feature-iad/dev-iad/sit-iad/prod-iad (set if AIRFLOW_SERVER_VERSION=2)
    """

    def __init__(self, lower_envs: List[str] = None, env_path="config", **kwargs):
        self.server_version = Variable.get(
            "AIRFLOW_SERVER_VERSION",
            default_var=int(environ.get("AIRFLOW_SERVER_VERSION", "1")),
        )
        self.envs = self.set_environments(lower_envs)
        self.kwargs = kwargs
        self.env_path = env_path
        self.dag_id = None
        self.file_path = get_return_caller_path()
        self.dags_home = get_dags_home(self.file_path)
        if self.dags_home not in sys_path:
            sys_path.append(self.dags_home)

    def __call__(self, f):
        @wraps(f)
        def add_env_to_dag(*args, **kwargs):
            output = None  # prevent errors if no output
            prod_enabled = self.kwargs.pop("prod_enabled", False)
            server_version = self.kwargs.get("params", {}).get("server_version")
            dag_id = self.kwargs.pop("dag_id", path.splitext(path.basename(self.file_path))[0])

            for environment in self.envs:
                env = Environment(environment, self.env_path, self.dags_home)

                # Set DAG parameters
                dag_kwargs = {
                    "dag_id": f"{dag_id}_{env.env}" if env.env in env.VALID_LOWER_ENVS else dag_id,
                    "access_control": env.DAG__ACCESS_CONTROL,
                    **env.DAG__DEFAULT_PARAMS,
                    **self.kwargs,
                    "default_args": {
                        **env.DAG__DEFAULT_ARGS,
                        **self.kwargs.get("default_args", {}),
                    }
                }

                # Set the environment obj as a kwarg of the actual DAG method for runtime
                kwargs["env"] = env

                if self.add_to_dagbag_by_environment(
                    env, prod_enabled
                ) and self.add_to_dagbag_by_server(env, server_version):
                    # now wrap the function with the modified dag decorator
                    output = dag(**dag_kwargs)(f)(*args, **kwargs)
                    logging.info(
                        f"Created DAG {dag_kwargs['dag_id']}, env: {env}, server: {server_version}"
                    )
            return output

        return add_env_to_dag

    def __iter__(self):
        self.env_iterator = iter(Environment(env) for env in self.envs)
        return self

    def __next__(self):
        return next(self.env_iterator)

    def set_environments(self, lower_envs: List[str] = None):
        """
        lower_envs: Any DAG may configure the list of lower environments it should run in.
        If no list is provided, it will default to TIER_NON and load environment variables from the non.py file.
        """
        if self.get_airflow_tier() == Environment.TIER_PROD:
            return [Environment.TIER_PROD]
        return self.get_dag_lower_environments(lower_envs)

    def get_airflow_tier(self) -> str:
        """
        Always return non or prod

        Server V1: CORE_AIRFLOW_ENVIRONMENT = non/prod
        Server V2: CORE_AIRFLOW_CLUSTER_CONTEXT = feature-iad/dev-iad/sit-iad/prod-iad

        """
        if self.server_version == 1:
            return Variable.get(
                "CORE_AIRFLOW_ENVIRONMENT",
                default_var=environ.get("CORE_AIRFLOW_ENVIRONMENT", "non"),
            ).lower()
        else:
            return (
                Environment.TIER_PROD
                if Variable.get(
                    "CORE_AIRFLOW_CLUSTER_CONTEXT",
                    default_var=environ.get("CORE_AIRFLOW_CLUSTER_CONTEXT", "non-iad"),
                )
                == "prod-iad"
                else Environment.TIER_NON
            )

    def get_dag_lower_environments(self, lower_envs: List[str] = None) -> List[str]:
        """Get the list of lower environments to run this DAG on from airflow variables

        Default to "non".
        """
        if not lower_envs:
            lower_envs = Environment.TIER_NON

        if not isinstance(lower_envs, list):
            lower_envs = [lower_envs]

        valid_lower_envs = Environment.VALID_LOWER_ENVS + [Environment.TIER_NON]

        if set(lower_envs).issubset(valid_lower_envs):
            return lower_envs
        else:
            raise ValueError("Invalid lower environment specified.")


    def add_to_dagbag_by_environment(self, env: Environment, prod_enabled: bool):
        return (
            env.tier_str == Environment.TIER_NON
            or env.env == Environment.TIER_NON
            or prod_enabled
        )

    def add_to_dagbag_by_server(self, env: Environment, use_server_version):
        return (
            self.server_version == 2
            and self.use_latest_server_version(env, use_server_version)
        ) or self.server_version == 1

    def use_latest_server_version(self, env, use_server_version):
        return env.env != env.TIER_PROD and use_server_version != 1


def get_return_caller_path() -> str:
    """
    Search the stack frame for the first farme that comes from a different file.

    This should be the one that defines the DAG.
    """
    for i, frame in enumerate(inspect.stack()):
        if not frame.filename.endswith(__file__):
            break
    return frame.filename


def get_dags_home(file_path: str) -> str:
    """
    Find the 'root' directory of the caller (dag)
    """
    match = re.search(r"(\S+dags_\w+[/\\])", file_path, re.IGNORECASE)
    return match[0] if match else path.dirname(file_path)
