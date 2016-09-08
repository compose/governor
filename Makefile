OUT := governor
PKG := github.com/compose/governor
PKG_LIST := $(shell go list ${PKG}/... | grep -v /vendor/)
GO_FILES := $(shell find . -name '*.go' | grep -v /vendor/)
VERSION := $(shell git describe --tags)
RELEASES := windows|amd64 linux|amd64 linux|arm

release: vet lint test build package-releases

package-releases:
	$(foreach release,$(RELEASES), \
		$(call release-version,$(firstword $(subst |, ,$(release))),\
					$(lastword $(subst |, ,$(release))))\
	)

define release-version
	@echo "Building release for $(strip $(1)):$(strip $(2))"
	mkdir -p ./build
	GOOS=$(strip $(1)) GOARCH=$(strip $(2)) go build -v -o ./build/${OUT} -ldflags "-X main.version=${VERSION}" ${PKG}
	tar -C ./build -cvzf ./build/governor-${VERSION}-$(strip $(1))-$(strip $(2)).tar.gz ${OUT}
	rm ./build/${OUT}

endef


build: build-native

build-native:
	go build -v -o ${OUT} -ldflags "-X main.version=${VERSION}" ${PKG}

vet:
	@go vet ${PKG_LIST}

test:
	@go test -cover -v ${PKG_LIST}

lint:
	@for file in ${GO_FILES}; do \
		golint $$file ; \
	done
