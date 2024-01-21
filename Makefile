# Exporting bin folder to the path for makefile
export PATH   := $(PWD)/bin:$(PATH)
# Default Shell
export SHELL  := bash
# Type of OS: Linux or Darwin.
export OSTYPE := $(shell uname -s)

ifeq ($(OSTYPE),Darwin)
    export MallocNanoZone=0
endif

include ./misc/makefile/tools.Makefile

build: test
	@go build ./...

setup:
	go install golang.org/x/tools/cmd/goimports@latest

install-deps: gotestsum tparse ## Install Development Dependencies (localy).
deps: $(GOTESTSUM) $(TPARSE) ## Checks for Global Development Dependencies.
deps:
	@echo "Required Tools Are Available"

TESTS_ARGS := --format testname --jsonfile gotestsum.json.out
TESTS_ARGS += --max-fails 2
TESTS_ARGS += -- ./...
TESTS_ARGS += -parallel 2
TESTS_ARGS += -count    1
TESTS_ARGS += -failfast
TESTS_ARGS += -coverprofile   coverage.out
TESTS_ARGS += -timeout        5m
TESTS_ARGS += -race
TESTS_ARGS += -short
run-tests: $(GOTESTSUM)
	@ gotestsum $(TESTS_ARGS)

test: run-tests $(TPARSE) ## Run Tests & parse details
	@cat gotestsum.json.out | $(TPARSE) -all -notests

pre-lint:
	 goimports -local github.com/golangci/golangci-lint -w .


lint: $(GOLANGCI) ## Runs golangci-lint with predefined configuration
	@echo "Applying linter"
	golangci-lint version
	golangci-lint run -c .golangci.yaml ./...


release:
	@sh scripts/release.sh



.PHONY: lint lint-prepare clean build unittest