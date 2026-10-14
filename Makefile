include common.mk

MARKDOWNLINT := website/node_modules/.bin/markdownlint-cli2
MARKDOWN_FILES = $(shell git ls-files -co --exclude-standard -- '*.md' '*.mdx')

.PHONY: all setup deps tidy tidy-check test test-v vet lint lint-fix fix fmt vuln build build-examples bench cover clean lint-docs lint-docs-fix ci

all: fmt vet lint test build build-examples

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

## Lint every Markdown file in the repo (needs Node)
lint-docs: $(MARKDOWNLINT)
	@$(MARKDOWNLINT) $(MARKDOWN_FILES)

## Lint Markdown and apply automatic fixes
lint-docs-fix: $(MARKDOWNLINT)
	@$(MARKDOWNLINT) --fix $(MARKDOWN_FILES)

$(MARKDOWNLINT): website/package-lock.json
	cd website && npm ci
	@touch $@

## The Go checks of the CI merge gate. Heavier than `make all` (needs the
## network for the advisory database). Markdown lint is separate: make lint-docs
ci: tidy-check vet lint test build-examples vuln
