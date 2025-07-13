import pickle
import base64
from airflow.models.param import DagParam
from airflow.models import BaseOperator
from airflow.providers.docker.operators.docker import DockerOperator
import inspect
from airflow import XComArg
from ast import literal_eval


class PythonicDockerOperator(DockerOperator):
    """
    Extension of the DockerOperator, allows pythonic passing of args to a container through the command parameter
    while maintaining python dtypes

    This [container repo](https://github.com/jfallt/dags-docker-template/tree/main) is a monolithic template to dynamically run python commands for a given module
    

    Parameters
    ----------
    module : str
        Name of the .py module to import from our container
    cmd_to_run : str
        Name of the method to run from the imported module
    cmd_args : dict
        Arguments to pass to the method
    environment : str
        Additional environment variables to pass to the container.
    secrets : dict
        Required secrets injected into the container as env vars from the secrets slice
    kwargs: str
        Any other parameters defined in the DockerOperator
    Returns
    -------
    Query output in dictionary or json
    """

    template_fields = DockerOperator.template_fields

    def __init__(
        self,
        env,
        module: str,
        cmd_to_run: str,
        cmd_args: dict = {},
        environment: dict = {},
        secrets: dict = {},
        *args,
        **kwargs,
    ):
        self.module = module
        self.cmd_to_run = cmd_to_run
        self.cmd_args = cmd_args
        self.kwargs = kwargs
        self.operator_args = PythonicDockerOperator.get_class_args(
            BaseOperator
        ) + VehiclesOperator.get_class_args(DockerOperator)

        self.update_cmd_args()
        self.command = self.generate_command()

        super().__init__(
            *args,
            **self.kwargs,
            **cdo_kwargs,
            secrets=secrets,
            # Rendering the command at runtime for visibility without pickling and encoding
            command=self.command,
            environment=environment,
        )

    def execute(self, context):
        self.command = self.pickle_and_encode_cmd_args()
        return super().execute(context)

    def generate_command(self):
        """
        The function does the following:
            - Outputs kwargs in a list format click can leverage
            - prevents Internal Server Error 500 from not casting inputs to strings
            - simplifies passing of args from Airflow to Docker container

        NOTE:
        This relies on all click args using dashes to separate words and the input to this function using underscores between words

        Example Usage:
            generate_command(module = 'fizz', cmd_to_run = 'buzz', arg_1 = 'x')

        Output:
            [--module, 'fizz', --cmd-to-run, 'buzz', --cmd-args, 'buzz', 'x']
        """
        docker_command = []
        kwargs = {}
        for command in [["module", self.module], ["cmd_to_run", self.cmd_to_run]]:
            docker_command.append(f"--{command[0].replace('_', '-')}")
            docker_command.append(str(command[1]))
        docker_command.append("--cmd-args")
        for key, arg in self.cmd_args.items():
            if isinstance(arg, bool):
                if not arg:
                    arg = ""
            kwargs[key] = str(arg)
            docker_command.append(key)
            docker_command.append(arg)
        return docker_command

    def pickle_and_encode_cmd_args(self):
        """
        Gathers the cmd_args, pickles them, and encodes them in base64 format to keep the python dtypes when passing to the container

        Command Example:
            [--module, 'sql', --cmd-to-run, 'with_output', --cmd-args, gASVbgEAAAAAAAB9lCiMA3NxbJSMTgogIL2JueXB4L3ByaWNpbmeUdS4=]
        """

        # Combine our command args in a dictionary
        cmd_args_index = self.command.index("--cmd-args")
        command_part1 = self.command[: cmd_args_index + 1]
        command_part2 = self.command[cmd_args_index + 1 :]
        cmd_args = dict(zip(command_part2[::2], command_part2[1::2]))

        for k, v in cmd_args.items():
            # if a jinja templated value is passed it's rendered as a string
            # and not a python native dtype, to make it easier to convert
            # these values in the container, we are going to convert them with
            # literal_eval now
            try:
                cmd_args[k] = literal_eval(v)
            except (ValueError, SyntaxError):
                pass

        # pickle and encode the cmd_args for later conversion inside our docker container
        command_part1.append(base64.b64encode(pickle.dumps(cmd_args)).decode("utf-8"))
        return command_part1

    @staticmethod
    def get_class_args(cls):
        """Function to get default arguments of a class's __init__ method"""
        signature = inspect.signature(cls.__init__)
        return [
            k for k, v in signature.parameters.items() if k not in ["self", "kwargs"]
        ]

    def update_cmd_args(self):
        """
        Remove any kwargs that are not expected in the base or docker operators
        and add them to the cmd_args, which get passed into the container
        """
        for key in list(self.kwargs.keys()):
            if key not in self.operator_args:
                self.cmd_args[key] = self.kwargs.pop(key)
