.PHONY: install-tools lint test

install-tools:
	go install golang.org/x/lint/golint
	go install github.com/kisielk/errcheck
	go install github.com/fzipp/gocyclo/cmd/gocyclo

lint:
	go fmt ./...
	golint ./...
	go vet ./...
	errcheck ./...
	gocyclo -over 10 .

test:
	go test -v ./...
