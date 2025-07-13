# DAGs Template
This repo is a template for setting up a modular, testable DAG infrastructure.

## Repo Structure

### .devcontainer/
Directory for local development

### commmon/

This directory is meant for reuseable code that can be used for any DAG.

Includes operators and task groups.

### config/

Environment Configs
- defaults.py: configs for variables / configs consistent across all envs
- non: a catchall for any variables that are the same across all lower environments
- prod: for Airflow's prod environment

### dags/

All DAGs go in this directory.

### hooks/
Custom pre-commit hook code. This helps us catch import errors, configuration errors etc.

#### Environment() and env_dag()

NOTE: this is in the airflow-plugins and we will leverage the plugin instead of our version in the near future

env_dag() is wrapper for the dag() decorator that will:

- Automatically determine whether it's running in Airflow non or prod and use the correct environment-based vars from config/
- create a DAG instance for every lower environment specified if it's running in non.
- Each DAG instance has access to an `env` parameter that is an instance of the `Environment` class. All environment specific variables are accessed as attributes of this object.

```

env = Environment('uat')  # this will pull variables from uat.py or non.py if none found.
env.MY_VAR  # access vars as properties
```

For any DAG, valid lower environments can be specified by passing a `lower_envs` parameter into env_dag() with a list of environments. It will default to `['dev', 'sit', 'uat']`.

See [the code](common/environment.py) for more details.

## Tests

#### Unit tests with Pytest

The DAG repo is meant to just contain the logic of job workflows. Business logic should ideally reside in python packages installed on a docker image.

However, any reusable code written to aid in the creation of job workflows should be tested.

Tests are automatically run with the pre-commit hooks
