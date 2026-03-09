export GCS_BUCKET=crabregistry
export BASE_URL=http://crabregistry.internal:8080
export AUTH_TOKEN=secret
export PORT=8080

.PHONY: build run
build:
	go build

run:
	go run main.go
