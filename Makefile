# Makefile for kafka-unwedge
BINARY := kafka-unwedge
DIST_DIR := dist
MODULE := github.com/x7ssss/kafka-unwedge

LDFLAGS := -s -w
GCFLAGS := -trimpath

.PHONY: all test build cross-build clean

all: test build

test:
	go test -v -race ./...

build:
	CGO_ENABLED=0 go build $(GCFLAGS) -ldflags="$(LDFLAGS)" -o $(BINARY) ./cmd/kafka-unwedge

cross-build:
	@mkdir -p $(DIST_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(GCFLAGS) -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/kafka-unwedge-linux-amd64 ./cmd/kafka-unwedge
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build $(GCFLAGS) -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/kafka-unwedge-linux-arm64 ./cmd/kafka-unwedge
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build $(GCFLAGS) -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/kafka-unwedge-darwin-amd64 ./cmd/kafka-unwedge
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build $(GCFLAGS) -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/kafka-unwedge-darwin-arm64 ./cmd/kafka-unwedge
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build $(GCFLAGS) -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/kafka-unwedge-windows-amd64.exe ./cmd/kafka-unwedge

clean:
	rm -rf $(DIST_DIR) $(BINARY) $(BINARY).exe
