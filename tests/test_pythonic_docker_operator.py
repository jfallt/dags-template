from operators.pythonic_docker_operator import PythonicDockerOperator
import pickle
import base64


def test_pythonic_docker_operator_initialization(mock_env):
    module = "test_module"
    cmd_to_run = "test_cmd"
    cmd_args = {"arg1": "value1"}
    environment = {"ENV_VAR": "env_value"}
    secrets = {"SECRET_KEY": "secret_value"}

    operator = PythonicDockerOperator(
        module=module,
        cmd_to_run=cmd_to_run,
        cmd_args=cmd_args,
        environment=environment,
        secrets=secrets,
        task_id="test_task",
        retrieve_output=True,
        retrieve_output_path="/tmp/xcom.json",
    )

    assert operator.module == module
    assert operator.cmd_to_run == cmd_to_run
    assert operator.cmd_args == cmd_args
    assert operator.environment == {**mock_env.ENV, **environment}
    assert operator.secrets == secrets
    assert operator.retrieve_output is True
    assert operator.retrieve_output_path == "/tmp/xcom.json"

    # This will show up in the airflow UI
    assert operator.command == [
        "--module",
        "test_module",
        "--cmd-to-run",
        "test_cmd",
        "--cmd-args",
        "arg1",
        "value1",
    ]

    # Checking the cmd-args are pickled and base64 encoded properly
    assert operator.pickle_and_encode_cmd_args() == [
        "--module",
        "test_module",
        "--cmd-to-run",
        "test_cmd",
        "--cmd-args",
        # cmd_args is base64 encoded to be passed as a string
        base64.b64encode(pickle.dumps(cmd_args)).decode("utf-8"),
    ]


def test_get_default_args():
    class TestClass:
        def __init__(self, arg1, arg2="default2", arg3="default3"):
            pass

    default_args = PythonicDockerOperator.get_class_args(TestClass)
    assert default_args == ["arg1", "arg2", "arg3"]
