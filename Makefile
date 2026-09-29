GOLANGCI_LINT_VERSION := v2.14.0
GOIMPORTS_VERSION := v0.50.0
GOVULNCHECK_VERSION := v1.8.0

.PHONY: all setup deps tidy tidy-check test test-v vet lint lint-fix fix fmt vuln print-govulncheck-version print-golangci-lint-version build build-examples bench cover clean ci

all: fmt vet lint test build build-examples

## Install development tools (skips if already present)
setup:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "Installing golangci-lint $(GOLANGCI_LINT_VERSION)..."; \
		go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION); \
	}
	@command -v goimports >/dev/null 2>&1 || { \
		echo "Installing goimports $(GOIMPORTS_VERSION)..."; \
		go install golang.org/x/tools/cmd/goimports@$(GOIMPORTS_VERSION); \
	}
	@command -v govulncheck >/dev/null 2>&1 || { \
		echo "Installing govulncheck $(GOVULNCHECK_VERSION)..."; \
		go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION); \
	}

## Download module dependencies
deps:
	go mod download

## Tidy go.mod/go.sum
tidy:
	go mod tidy

## Fail if go.mod/go.sum is not tidy, printing the diff without modifying
## anything. Suitable for CI, where a stale go.mod should block the merge.
tidy-check:
	go mod tidy -diff

## Run all tests with race detector
test:
	go test -race -count=1 ./...

## Run tests with verbose output
test-v:
	go test -race -v -count=1 ./...

## Format code
fmt:
	gofmt -s -w .
	goimports -w .

## Run go vet
vet:
	go vet ./...

## Run golangci-lint
lint: setup
	golangci-lint run ./...

## Run golangci-lint with auto-fix
lint-fix: setup
	golangci-lint run --fix ./...

## Apply `go fix` modernizers, then fix formatting and lint issues
fix:
	go fix ./...
	@$(MAKE) --no-print-directory fmt lint-fix

## Scan for known vulnerabilities, filtered to advisories this code actually
## reaches. Needs network access — the advisory database is fetched on every run.
##
## This also scans the standard library of whichever Go toolchain you have
## installed, so it can fail locally on a green branch when your Go is a patch
## release behind the one CI uses. That is a real finding about your machine,
## not a false positive.
vuln: setup
	govulncheck ./...

## Print the pinned scanner version, so CI and this file cannot drift apart.
print-govulncheck-version:
	@echo $(GOVULNCHECK_VERSION)

## Print the pinned linter version, so CI and this file cannot drift apart.
print-golangci-lint-version:
	@echo $(GOLANGCI_LINT_VERSION)

## Build all packages
build:
	go build ./...

## Build every example program
build-examples:
	@for dir in examples/*/; do \
		echo "==> build $$dir"; \
		go build -o /dev/null "./$$dir" || exit 1; \
	done

## Run benchmarks
bench:
	go test -bench=. -benchmem -count=3 -run='^$$' ./...

## Run tests with coverage report
cover:
	go test -race -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

## Remove build artifacts
clean:
	rm -f coverage.out coverage.html

## Everything the CI merge gate checks, in one target. Heavier than `make all`
## (needs the network for the advisory database); run it before opening a PR.
ci: tidy-check vet lint test build-examples vuln
