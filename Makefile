PACKAGES := api consumer data migrations relay
TESTS := $(addprefix test-,$(PACKAGES))
UV ?= uv

.PHONY: install format lint test check up down load $(TESTS)

install:
	$(UV) sync --all-packages

format:
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

lint:
	$(UV) run ruff format --check .
	$(UV) run ruff check .

test: $(TESTS)

$(TESTS): test-%:
	cd $* && $(UV) run --package $* pytest

check: lint test

up:
	docker compose up -d --build --wait api relay consumer

down:
	docker compose down

load:
	docker compose up -d webhook-sink
	$(UV) run python loadtest/load_test.py $(ARGS)
