.PHONY: build clean test loadapp-image

test:
	go test -race -cover ./...

build:
	go build -o bin/controld    ./cmd/controld
	go build -o bin/noded      ./cmd/noded
	go build -o bin/proxyd     ./cmd/proxyd
	go build -o bin/helios     ./cmd/helios
	go build -o bin/trafficgen ./cmd/trafficgen
	go build -o bin/loadapp    ./cmd/loadapp

# Container image for the experiment workload (scripts/experiment.sh).
loadapp-image:
	docker build -f Dockerfile.loadapp -t helios-loadapp:latest .

clean:
	rm -rf bin
