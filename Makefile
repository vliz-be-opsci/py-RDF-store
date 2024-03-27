TEST_PATH = ./tests/
FLAKE8_EXCLUDE = venv,.venv,.eggs,.tox,.git,__pycache__,*.pyc
PROJECT = pyrdfstore
AUTHOR = "Cedric Decruw"#

clean:
	@find . -name '*.pyc' -exec rm --force {} +
	@find . -name '*.pyo' -exec rm --force {} +
	@find . -name '*~' -exec rm --force {} +
	@rm -rf build
	@rm -rf dist
	@rm -rf *.egg-info
	@rm -f *.sqlite
	@rm -rf .cache

startup:
	pip install --upgrade pip
	which poetry >/dev/null || pip install poetry

install:
	poetry install

init: startup install

init-dev: startup
	poetry install --with 'tests' --with 'dev' --with 'docs'

init-docs: startup
	poetry install --with 'docs'

docs:
	if ! [ -d "./docs" ]; then poetry run sphinx-quickstart -q --ext-autodoc --ext-githubpages --ext-viewcode --sep --project $(PROJECT) --author '${AUTHOR}' docs; fi
	cp ./source_docs/* ./docs/source/
	sleep 1
	poetry run sphinx-apidoc -o ./docs/source ./$(PROJECT)
	poetry run sphinx-build -b html ./docs/source ./docs/build/html
	cp ./docs/source/custom.css ./docs/build/html/_static/custom.css
test:
	poetry run pytest ${TEST_PATH}

test-coverage:
	poetry run pytest --cov=$(PROJECT) ${TEST_PATH} --cov-report term-missing

check:
	poetry run black --check --diff .
	poetry run isort --check --diff .
	poetry run flake8 . --exclude ${FLAKE8_EXCLUDE} --ignore=E501,E201,E202,W503

lint-fix:
	poetry run black .
	poetry run isort .

update:
	poetry update

build: update check test docs
	poetry build

release: build
	poetry release