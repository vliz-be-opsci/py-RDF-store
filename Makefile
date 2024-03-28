TEST_PATH = ./tests/
FLAKE8_EXCLUDE = venv,.venv,.eggs,.tox,.git,__pycache__,*.pyc
PROJECT = pyrdfstore
AUTHOR = "Flanders Marine Institute, VLIZ vzw"

REPONAME = py_rdf_store_test

.PHONY: help docker-build docker-push docker-start docker-stop
.DEFAULT_GOAL := help

help:  ## Shows this list of available targets and their effect.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

clean:  ## removes all possible derived built results from other executions of the make
	@find . -name '*.pyc' -exec rm --force {} +
	@find . -name '*.pyo' -exec rm --force {} +
	@find . -name '*~' -exec rm --force {} +
	@rm -rf build
	@rm -rf dist
	@rm -rf *.egg-info
	@rm -f *.sqlite
	@rm -rf .cache

startup: ## prepares environment for using poetry (a core dependency for this project)
	@pip install --upgrade pip
	@which poetry >/dev/null || pip install poetry

install:  ## install this package in the current environment
	@poetry install

init: startup install  ## initial prepare of the environment for local execution of the package

init-dev: startup  ## initial prepare of the environment for further development in the package
	@poetry install --with 'tests' --with 'dev' --with 'docs'

init-docs: startup  ## initial prepare of the environment for local execution and reading the docs
	@poetry install --with 'docs'

docs:  ## builds the docs
	@if ! [ -d "./docs" ]; then poetry run sphinx-quickstart -q --ext-autodoc --ext-githubpages --ext-viewcode --sep --project $(PROJECT) --author '${AUTHOR}' docs; fi
	@cp ./pre_docs/* ./docs/source/
	@sleep 1
	@poetry run sphinx-apidoc -o ./docs/source ./$(PROJECT)
	@poetry run sphinx-build -b html ./docs/source ./docs/build/html
	@cp ./docs/source/custom.css ./docs/build/html/_static/custom.css

test:  ## runs the standard test-suite for the memory-graph implementation
	@poetry run pytest ${TEST_PATH}

test-quick:  ## runs tests more quickly by skipping some lengthy ones
	@(export QUICKTEST=1 && $(MAKE) test --no-print-directory)

test-with-graphdb: ## runs the standard test-suite for all available implementations (requires docker to spin up a sparql endpoint)
	@(export REPONAME=${REPONAME} && ./tests/kgap-graphdb.sh start-wait)
	-@(export TEST_SPARQL_READ_URI=http://localhost:7200/repositories/${REPONAME} TEST_SPARQL_WRITE_URI=http://localhost:7200/repositories/${REPONAME}/statements && $(MAKE) test --no-print-directory)
	@./tests/kgap-graphdb.sh stop

test-coverage:  ## runs the standard test-suite for the memory-graph implementation and produces a coverage report
	@poetry run pytest --cov=$(PROJECT) ${TEST_PATH} --cov-report term-missing

test-coverage-with-graphdb:  ## runs the standard test-suite for all available implementations and produces a coverage report
	@(export REPONAME=${REPONAME} && ./tests/kgap-graphdb.sh start-wait)
	-@(export TEST_SPARQL_READ_URI=http://localhost:7200/repositories/${REPONAME} TEST_SPARQL_WRITE_URI=http://localhost:7200/repositories/${REPONAME}/statements && $(MAKE) test-coverage --no-print-directory)
	@./tests/kgap-graphdb.sh stop

check:  ## performs linting on the python code
	@poetry run black --check --diff .
	@poetry run isort --check --diff .
	@poetry run flake8 . --exclude ${FLAKE8_EXCLUDE} --ignore=E501,E201,E202,W503

lint-fix:  ## fixes code according to the lint suggestions
	@poetry run black .
	@poetry run isort .

update:  ## updates dependencies
	@poetry update

build: update check test docs  ## builds the package
	@poetry build

release: build  ## releases the package
	@poetry release