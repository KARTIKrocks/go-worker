.PHONY: test test-race bench lint cover tidy check

## Run tests
test:
	go test -count=1 ./...

## Run tests with race detector
test-race:
	go test -race -count=1 ./...

## Run benchmarks
bench:
	go test -bench=. -benchmem -count=3 ./...

## Run linter (requires golangci-lint)
lint:
	golangci-lint run ./...

## Generate coverage report
cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html

## Tidy modules
tidy:
	go mod tidy

## Run all checks (test + race + lint)
check: tidy lint test-race
