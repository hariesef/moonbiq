go-gen:
	rm -rf internal/mocks/*
	go generate ./...

test:
	go test -v -cover ./internal/storage/input/mongo	
	go test -v -cover ./internal/storage/output/bigquery	