#!/bin/bash

# Environment setup
pipx install poetry
pipx inject poetry poetry-plugin-shell
poetry install
poetry install --with dev

sudo apt install pre-commit -y
pre-commit install --install-hooks

# Ensure our poetry environment is activated and our plugins are in the PYTHONPATH
sudo echo -e 'source .venv/bin/activate' >> ~/.bashrc
