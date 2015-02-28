test:
	GOMAXPROCS=1 go test
	GOMAXPROCS=2 go test
	GOMAXPROCS=4 go test
	GOMAXPROCS=8 go test

bench-1-goprocs:
	GOMAXPROCS=1 go test -test.bench=".*"

bench-2-goprocs:
	GOMAXPROCS=2 go test -test.bench=".*"

bench-4-goprocs:
	GOMAXPROCS=4 go test -test.bench=".*"

bench-8-goprocs:
	GOMAXPROCS=8 go test -test.bench=".*"

