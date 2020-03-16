test: deps
	go test -v -coverprofile=cover.out .

deps:
	go get ./...