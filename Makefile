# SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
# SPDX-License-Identifier: Apache-2.0
# Makefile for xmidt-syslog-bridge

.PHONY: help test test-integration test-all coverage lint vet fmt tidy clean

.DEFAULT_GOAL := help

# This Makefile is the interface to the tests.  Call it rather than `go test`,
# for two reasons.
#
# The first is the `integration` build tag.  Everything that needs a container
# lives in a *_integration_test.go file behind it, so that the unit suite stays
# fast enough to run on every save -- seconds rather than minutes, which is what
# the red-green loop depends on.  A bare `go test ./...` therefore runs only
# half the suite, and says nothing about the half it skipped.
#
# The second is the environment the container tests need, which is otherwise
# three variables nobody remembers.  Rootless podman, matching
# xmidt-org/wrpkafka.  On an SELinux host (Fedora, RHEL) testcontainers' reaper
# mounts the container socket and SELinux does not let the container_t domain
# connectto the daemon, so the reaper is denied whatever uid it runs as; it is
# turned off here.  Nothing leaks from that: the fixture terminates its own
# container in TestMain.
#
# To use Docker instead, override the socket:
#
#   make test-integration CONTAINER_SOCK=unix:///var/run/docker.sock
#
# or keep the reaper and run it unconfined, which also works under SELinux:
#
#   TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED=true make test-integration
#
# The socket is chosen by what is actually there, so that the same target works
# on a developer's podman box and on a CI runner that only has Docker.
PODMAN_SOCK := /run/user/$(shell id -u)/podman/podman.sock

CONTAINER_SOCK ?= $(if $(wildcard $(PODMAN_SOCK)),unix://$(PODMAN_SOCK),unix:///var/run/docker.sock)

CONTAINER_ENV := \
	DOCKER_HOST=$(CONTAINER_SOCK) \
	TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=$(CONTAINER_SOCK) \
	TESTCONTAINERS_RYUK_DISABLED=true

INTEGRATION_TAG := integration

# -count=1 defeats Go's test cache.  Without it a repeat run replays cached
# results in a fraction of a second and still prints `ok`, which reads exactly
# like a suite that ran and passed -- only a small `(cached)` marker says
# otherwise.  These targets exist to tell you whether the tests pass now, so
# they always actually run them.
COUNT := -count=1

## test: Run the unit suite -- no containers, fast enough for every save
test:
	@echo "Running unit tests..."
	@go test -race $(COUNT) ./...

## test-integration: Run the integration suite -- needs a container runtime
test-integration:
	@echo "Running integration tests (needs a container runtime)..."
	@$(CONTAINER_ENV) go test -race $(COUNT) -tags=$(INTEGRATION_TAG) -timeout 15m ./...

## test-all: Run the unit suite, then the integration suite
test-all: test test-integration

## coverage: Generate an HTML coverage report from both suites
coverage:
	@echo "Generating coverage report..."
	@$(CONTAINER_ENV) go test -race $(COUNT) -tags=$(INTEGRATION_TAG) -timeout 15m \
		-coverprofile=coverage.out -covermode=atomic ./...
	@go tool cover -func=coverage.out | tail -1
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

## vet: Run go vet over both build configurations
vet:
	@echo "Vetting the unit build..."
	@go vet ./...
	@echo "Vetting the integration build..."
	@go vet -tags=$(INTEGRATION_TAG) ./...

## lint: Run golangci-lint over both build configurations
lint:
	@echo "Linting the unit build..."
	@golangci-lint run ./...
	@echo "Linting the integration build..."
	@golangci-lint run --build-tags=$(INTEGRATION_TAG) ./...

## fmt: Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...

## tidy: Tidy go modules
tidy:
	@echo "Tidying go modules..."
	@go mod tidy

## clean: Remove generated files
clean:
	@echo "Cleaning up..."
	@rm -f coverage.out coverage.html

## help: Show this help message
help:
	@echo "Available targets:"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
