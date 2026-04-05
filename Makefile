install:
	poetry install

run:
	poetry run python -m app.main

fmt:
	poetry run ruff format .

lint:
	poetry run ruff check .

lint-fix:
	poetry run ruff check . --fix

test:
	poetry run pytest

check: lint test