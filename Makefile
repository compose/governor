OUT := governor
PKG := github.com/compose/governor
PKG_LIST := $(shell go list ${PKG}/... | grep -v /vendor/)
GO_FILES := $(shell find . -name '*.go' | grep -v /vendor/)
VERSION := $(shell git describe --tags)

release: vet lint test build

build:
	go build -v -o ${OUT} -ldflags "-X main.version=${VERSION}" ${PKG}

vet:
	@go vet ${PKG_LIST}

test:
	@go test -cover -v ${PKG_LIST}

lint:
	@for file in ${GO_FILES}; do \
		golint $$file ; \
	done
