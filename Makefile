.PHONY: clean prepare test

help:
	@echo 'Perform common development tasks'
	@echo 'Usage: make [TARGET]'
	@echo 'Targets:'
	@echo '  clean			Clean removes the vendor directory, go.mod, and go.sum files'
	@echo '  prepare		Sets up a go.mod, go.sum and downloads all vendor dependencies'
	@echo '  test			Starts a dynamo local dynamo container and runs unit and integration tests'
	@echo ''

clean:
	-docker-compose down
	@rm -rf vendor go.sum go.mod coverage.out

prepare:
	@go mod init github.com/edwardsmatt/dynamocity
	@git config --global url."git@github.com:".insteadOf "https://github.com/"
	@go get github.com/aws/aws-sdk-go-v2@v0.24.0
	@go mod download
	@go mod vendor
	@go mod tidy

test:
	@docker-compose up -d
	-go test -v github.com/edwardsmatt/dynamocity -coverprofile=coverage.out
	@docker-compose down