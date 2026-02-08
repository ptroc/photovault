SHELL := /bin/bash

VENV ?= .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
RUFF := $(VENV)/bin/ruff
PYTEST := $(VENV)/bin/pytest

SERVICE_DIRS := \
	services/photovault-clientd \
	services/photovault-client-ui \
	services/photovault-api \
	services/photovault-server-ui

TEST_DIRS := \
	services/photovault-clientd/tests \
	services/photovault-client-ui/tests \
	services/photovault-api/tests \
	services/photovault-server-ui/tests

.PHONY: help venv install install-dev lint test check clean

help:
	@echo "Available targets:"
	@echo "  make venv        - Create local virtual environment at .venv"
	@echo "  make install     - Install dependencies from requirements.txt"
	@echo "  make install-dev - Install dependencies from requirements-dev.txt"
	@echo "  make lint        - Run ruff across services"
	@echo "  make test        - Run pytest across all service test suites"
	@echo "  make check       - Run lint + test"
	@echo "  make clean       - Remove Python cache files"

venv:
	python3 -m venv $(VENV)

install: venv
	$(PIP) install -r requirements.txt

install-dev: venv
	$(PIP) install -r requirements-dev.txt

lint:
	$(RUFF) check $(SERVICE_DIRS)

test:
	$(PYTEST) $(TEST_DIRS)

check: lint test

clean:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type f -name '*.py[co]' -delete
