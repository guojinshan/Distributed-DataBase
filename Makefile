SHELL = /bin/bash

.DEFAULT_GOAL := all

## help: Display list of commands
.PHONY: help
help: Makefile
	@sed -n 's|^##||p' $< | column -t -s ':' | sed -e 's|^| |'

## all: Run all targets
.PHONY: all
all: init style test

## init: Bootstrap your application.
.PHONY: init
init:
	pre-commit install -t pre-commit
	pipenv install --dev

## format: Format code.
## style: Check lint, code styling rules.
.PHONY: format
.PHONY: style
style format:
	pre-commit run -a

## test: Run all the tests.
.PHONY: test
test:
	PYTHONPATH=. pipenv run py.test tests/

## test-bare: Run the test for the bare implementation
test-bare:
	PYTHONPATH=. pipenv run py.test tests/test_crud_storage.py

## test-base: Run the test for the base implementation.
test-base:
	PYTHONPATH=. pipenv run py.test tests/ \
				-k test_consistent_hashing_crud_storage \
				-k TestConsistentHashingCrudStorage

## test-add: Run the test for the shard addition.
.PHONY: test-add
test-add:
	PYTHONPATH=. pipenv run py.test tests/ -k add

## test-remove: Run the test for the shard removal.
.PHONY: test-remove
test-remove:
	PYTHONPATH=. pipenv run py.test tests/ -k remove

## run-distributions: Shortcut to run the benchmark python script.
.PHONY: run-distributions
run-distributions:
	PYTHONPATH=. pipenv run python key_value_part2/distributions.py

## local-ci: Make sure the content of this repository is great.
.PHONY: local-ci
local-ci:
	docker build --rm -f ci.Dockerfile .

## clean: Remove temporary files
.PHONY: clean
clean:
	-pipenv --rm
