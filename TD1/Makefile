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

## test: Shortcut to launch all the test tasks (unit, functional and integration).
.PHONY: test
test:
	PYTHONPATH=. pipenv run py.test tests/

## run-benchmark: Shortcut to run the benchmark python script.
.PHONY: run-benchmark
run-benchmark:
	PYTHONPATH=. pipenv run python key_value_part1/benchmark.py

## local-ci: Make sure the content of this repository is great.
.PHONY: local-ci
local-ci:
	docker build --rm -f ci.Dockerfile .

## clean: Remove temporary files
.PHONY: clean
clean:
	-pipenv --rm
