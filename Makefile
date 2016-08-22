OUT := governor
PKG := github.com/compose/governor
PKG_LIST := $(shell go list ${PKG}/... | grep -v /vendor/)
GO_FILES := $(shell find . -name '*.go' | grep -v /vendor/)
VERSION := $(shell git describe --tags)

build: vet lint test
	go build -v -o ${OUT} -ldflags "-X main.version=${VERSION}" ${PKG}

vet:
	@go vet ${PKG_LIST}

test:
	@go test ${PKG_LIST}

lint:
	@for file in ${GO_FILES}; do \
		golint $$file ; \
	done
