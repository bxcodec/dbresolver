test: 
	@go test -v ./... -cover -race -coverprofile=coverage.txt -covermode=atomic

unittest:
	@go test -v -short ./...

# Linter
lint-prepare: 
	@echo "Installing golangci-lint"
	# @go get -u github.com/golangci/golangci-lint/cmd/golangci-lint 
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s latest

lint: 
	./bin/golangci-lint run ./...