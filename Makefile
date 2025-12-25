.PHONY: build test test-race test-coverage test-integration integration-up integration-down clean

build:
	go build -v ./...

test:
	go test -v ./...

test-race:
	go test -v -race ./...

test-coverage:
	go test -coverpkg=./... ./... -race -coverprofile=coverage.out -covermode=atomic
	go tool cover -html=coverage.out -o coverage.html

test-integration: integration-up
	CONSUL_HTTP_ADDR=http://localhost:8500 go test -v -tags=integration -race ./...
	$(MAKE) integration-down

integration-up:
	docker compose -f configs/integration/docker-compose.yaml up -d --wait

integration-down:
	docker compose -f configs/integration/docker-compose.yaml down -v

clean:
	rm -f coverage.out coverage.html
	go clean -testcache
