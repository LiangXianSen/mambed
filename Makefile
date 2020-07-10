GO=go

SRC = $(shell find . -type f -name '*.go' -not -path "./vendor/*")
project = github.com/LiangXianSen/mambed
packages := $(shell go list ./...|grep -v /vendor/)

.PHONY: check test lint

test: check
	@$(GO) test $(packages) -v -coverprofile=.coverage.out
	@$(GO) tool cover -func=.coverage.out
	@rm -f .coverage.out

check:
	@$(GO) vet -composites=false $(packages)

lint:
	@golint -set_exit_status ./...

doc:
	@godoc -http=localhost:8098

clean:
	@rm $(TARGET)
