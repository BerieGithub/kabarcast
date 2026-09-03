.PHONY: run build test tidy docker-up docker-down fmt vet

run:
	go run ./cmd/kabarcast

build:
	go build -o bin/kabarcast ./cmd/kabarcast

test:
	go test ./... -race -cover

tidy:
	go mod tidy

fmt:
	go fmt ./...

vet:
	go vet ./...

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down
